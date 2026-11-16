"""分群画像增量引擎：编排层。

并发与唯一性保证（对应「同一个分群的分区并发到达时只允许一个完整画像快照生效」）：
- 每个分群一把可重入锁，分区批次的接收、合并、快照生效在同一把锁内串行完成；
- 批次按 batch_id 幂等：重复提交直接返回首次结果，不会产生第二份快照；
- 快照按 fingerprint 幂等：同输入同指纹，存储层不允许同一窗口出现两份等效快照；
- 窗口只维护一个 effective 快照指针，新版本生效时旧快照标 superseded 但不删除。

失败恢复（对应「失败后从缺失分区恢复」）：
- 窗口按规则的 expected_partitions 判定完整性，缺失/失败分区可在任意时刻补提；
- 补提走与首次到达完全相同的合并路径，补齐后自动固化完整快照。
"""
from __future__ import annotations

import threading
from dataclasses import asdict
from typing import Any

from app.segment.boundary import DEFAULT_POLICY, POLICIES, resolve_window
from app.segment.invalidation import (
    invalidate_for_partition,
    snapshot_supersede_scope,
)
from app.segment.merge import (
    MergeAccumulator,
    build_snapshot,
    compute_coverage_diff,
    new_accumulator,
)
from app.segment.models import (
    AuditEvent,
    BoundaryConflict,
    CoverageDiff,
    PartitionBatch,
    PartitionState,
    PopulationRecord,
    SegmentRule,
    Snapshot,
    WindowState,
    new_id,
    utc_now,
)
from app.segment.repository import (
    PopulationRepository,
    RuleRegistry,
    SnapshotRepository,
)


class SegmentProfileEngine:
    def __init__(
        self,
        population: PopulationRepository,
        rules: RuleRegistry,
        snapshots: SnapshotRepository,
    ) -> None:
        self.population = population
        self.rules = rules
        self.snapshots = snapshots
        self._locks: dict[str, threading.RLock] = {}
        self._lock_guard = threading.Lock()
        # segment_ref -> window_id -> 运行态
        self.windows: dict[str, dict[str, WindowState]] = {}
        # (segment_ref, window_id) -> 累加器（随窗口长期保留，供恢复与重放）
        self.accumulators: dict[tuple[str, str], MergeAccumulator] = {}
        # batch_id -> 首次处理结果（输入级幂等）
        self.batch_index: dict[str, dict[str, Any]] = {}
        self.conflicts: dict[str, list[BoundaryConflict]] = {}
        # segment_ref -> [(snapshot_id, window_id, 成员集合)]，按生效顺序追加，
        # 既是覆盖对象差异的跨窗口基准链，也是成员明细的追溯来源
        self.member_chain: dict[str, list[tuple[str, str, dict[str, str]]]] = {}
        self.audit_log: list[AuditEvent] = []

    # ------------------------------------------------------------------ 基础
    def _segment_lock(self, segment_ref: str) -> threading.RLock:
        with self._lock_guard:
            lock = self._locks.get(segment_ref)
            if lock is None:
                lock = threading.RLock()
                self._locks[segment_ref] = lock
            return lock

    def _audit(
        self,
        action: str,
        *,
        segment_ref: str | None,
        window_id: str | None,
        partition: str | None,
        operator: str,
        detail: dict[str, Any],
    ) -> AuditEvent:
        event = AuditEvent(
            event_id=new_id("audit"),
            created_at=utc_now(),
            action=action,
            segment_ref=segment_ref,
            window_id=window_id,
            partition=partition,
            operator=operator,
            detail=detail,
        )
        self.audit_log.append(event)
        return event

    def register_segment(
        self,
        *,
        segment_ref: str,
        segment_name: str,
        expression: dict[str, Any],
        profile_dimensions: tuple[str, ...],
        expected_partitions: tuple[str, ...],
        boundary_policy: str = DEFAULT_POLICY,
        operator: str = "system",
    ) -> SegmentRule:
        if boundary_policy not in POLICIES:
            raise ValueError(f"边界策略 {boundary_policy} 不合法，允许 {POLICIES}")
        rule = self.rules.register(
            segment_ref=segment_ref,
            segment_name=segment_name,
            expression=expression,
            profile_dimensions=profile_dimensions,
            expected_partitions=expected_partitions,
            boundary_policy=boundary_policy,
        )
        self._audit(
            "rule_register",
            segment_ref=segment_ref,
            window_id=None,
            partition=None,
            operator=operator,
            detail={
                "rule_version": rule.version,
                "expression": rule.expression,
                "expected_partitions": list(rule.expected_partitions),
                "boundary_policy": rule.boundary_policy,
            },
        )
        return rule

    # ------------------------------------------------------------- 批次接入
    def ingest_batch(self, batch: PartitionBatch, *, operator: str = "system") -> dict[str, Any]:
        """接收一个分区批次。线程安全、批次幂等、窗口完整即固化快照。"""
        rule = self.rules.latest(batch.segment_ref)
        if rule is None:
            raise ValueError(f"分群 {batch.segment_ref} 尚未注册筛选规则，拒绝接入批次")
        lock = self._segment_lock(batch.segment_ref)
        with lock:
            duplicate = self.batch_index.get(batch.batch_id)
            if duplicate is not None:
                self._audit(
                    "batch_duplicate_ignored",
                    segment_ref=batch.segment_ref,
                    window_id=duplicate.get("window_id"),
                    partition=batch.partition,
                    operator=operator,
                    detail={"batch_id": batch.batch_id, "first_result": duplicate["status"]},
                )
                return {**duplicate, "duplicate": True}

            segment_windows = self.windows.setdefault(batch.segment_ref, {})
            window_id, conflict = resolve_window(batch, rule, segment_windows)

            if batch.partition not in rule.expected_partitions:
                raise ValueError(
                    f"分区 {batch.partition} 不在分群预期分区 "
                    f"{list(rule.expected_partitions)} 内，拒绝接入"
                )

            window = segment_windows.get(window_id)
            if window is None:
                window = WindowState(segment_ref=batch.segment_ref, window_id=window_id)
                segment_windows[window_id] = window
            accumulator = self.accumulators.get((batch.segment_ref, window_id))
            if accumulator is None:
                accumulator = new_accumulator(rule, window_id)
                self.accumulators[(batch.segment_ref, window_id)] = accumulator

            records: list[PopulationRecord] = self.population.get_many(batch.object_ids)
            prior_state = window.partitions.get(batch.partition)
            try:
                if prior_state is None:
                    trace = accumulator.add_partition(batch, records, rule)
                else:
                    # 分区已有批次（失败恢复或重算）：回滚旧贡献后复用同一条合并路径
                    trace = accumulator.replace_partition(batch, records, rule)
            except ValueError as exc:
                state = window.partitions.get(batch.partition)
                if state is not None:
                    state.status = "failed"
                self._audit(
                    "batch_merge_rejected",
                    segment_ref=batch.segment_ref,
                    window_id=window_id,
                    partition=batch.partition,
                    operator=operator,
                    detail={"batch_id": batch.batch_id, "reason": str(exc)},
                )
                raise

            attempts = prior_state.attempts + 1 if prior_state is not None else 1
            window.partitions[batch.partition] = PartitionState(
                window_id=window_id,
                partition=batch.partition,
                status="received",
                batch_id=batch.batch_id,
                object_count=trace.matched_count,
                event_time=batch.event_time,
                arrival_time=batch.arrival_time,
                attempts=attempts,
            )
            if batch.batch_id not in window.batch_ids:
                window.batch_ids.append(batch.batch_id)

            conflict_id = None
            if conflict is not None:
                self.conflicts.setdefault(batch.segment_ref, []).append(conflict)
                conflict_id = conflict.conflict_id

            self._audit(
                "batch_ingested",
                segment_ref=batch.segment_ref,
                window_id=window_id,
                partition=batch.partition,
                operator=operator,
                detail={
                    "batch_id": batch.batch_id,
                    "incoming": trace.incoming_count,
                    "matched": trace.matched_count,
                    "unknown_ids": trace.unknown_ids,
                    "duplicate_objects": trace.duplicate_object_ids,
                    "boundary_conflict_id": conflict_id,
                },
            )

            result: dict[str, Any] = {
                "status": "accepted",
                "duplicate": False,
                "batch_id": batch.batch_id,
                "window_id": window_id,
                "partition": batch.partition,
                "missing_partitions": window.missing_partitions(rule.expected_partitions),
                "boundary_conflict_id": conflict_id,
            }

            if window.is_complete(rule.expected_partitions):
                published = self._finalize_snapshot(
                    rule, window, accumulator,
                    changed_partitions=(batch.partition,),
                    operator=operator,
                )
                result["status"] = "snapshot_published"
                result["snapshot_id"] = published["snapshot_id"]
                result["diff_id"] = published.get("diff_id")
                result["fingerprint"] = published.get("fingerprint")

            self.batch_index[batch.batch_id] = result
            return result

    def mark_partition_failed(
        self, segment_ref: str, window_id: str, partition: str, *, operator: str = "system"
    ) -> dict[str, Any]:
        """把分区标记为失败（模拟上游故障），恢复时该分区会被识别为缺失。"""
        with self._segment_lock(segment_ref):
            window = self.windows.get(segment_ref, {}).get(window_id)
            if window is None or partition not in window.partitions:
                raise ValueError(f"窗口 {window_id} 下不存在分区 {partition}，无法标记失败")
            window.partitions[partition].status = "failed"
            window.partitions[partition].attempts += 1
            window.effective_snapshot_id = None
            self._audit(
                "partition_marked_failed",
                segment_ref=segment_ref,
                window_id=window_id,
                partition=partition,
                operator=operator,
                detail={"attempts": window.partitions[partition].attempts},
            )
            rule = self.rules.latest(segment_ref)
            return {
                "window_id": window_id,
                "partition": partition,
                "missing_partitions": window.missing_partitions(
                    rule.expected_partitions if rule else ()
                ),
            }

    def recover_window(
        self, segment_ref: str, window_id: str, batches: list[PartitionBatch], *,
        operator: str = "system",
    ) -> dict[str, Any]:
        """从缺失/失败分区恢复：补提批次，复用同一条合并路径，补齐后出完整快照。"""
        rule = self.rules.latest(segment_ref)
        if rule is None:
            raise ValueError(f"分群 {segment_ref} 未注册规则")
        with self._segment_lock(segment_ref):
            window = self.windows.get(segment_ref, {}).get(window_id)
            if window is None:
                raise ValueError(f"窗口 {window_id} 不存在，无可恢复状态")
            missing_before = set(window.missing_partitions(rule.expected_partitions))
            self._audit(
                "window_recover_started",
                segment_ref=segment_ref,
                window_id=window_id,
                partition=None,
                operator=operator,
                detail={"missing_partitions": sorted(missing_before), "batch_count": len(batches)},
            )
            ingested: list[dict[str, Any]] = []
            for batch in batches:
                if batch.segment_ref != segment_ref:
                    raise ValueError("恢复批次必须属于同一分群")
                ingested.append(self.ingest_batch(batch, operator=operator))
            missing_after = window.missing_partitions(rule.expected_partitions)
            return {
                "window_id": window_id,
                "recovered_partitions": sorted(missing_before - set(missing_after)),
                "missing_partitions": missing_after,
                "complete": not missing_after,
                "effective_snapshot_id": window.effective_snapshot_id,
                "ingested": ingested,
            }

    # ------------------------------------------------------------- 快照固化
    def _finalize_snapshot(
        self,
        rule: SegmentRule,
        window: WindowState,
        accumulator: MergeAccumulator,
        *,
        changed_partitions: tuple[str, ...],
        operator: str,
    ) -> dict[str, Any]:
        segment_ref = rule.segment_ref
        window_id = window.window_id

        conflict_ids = tuple(
            c.conflict_id for c in self.conflicts.get(segment_ref, [])
            if c.window_id == window_id
        )
        next_version = len(self.snapshots.list_snapshots(segment_ref)) + 1
        candidate = build_snapshot(
            accumulator, version=next_version, boundary_conflict_ids=conflict_ids
        )

        existing = self.snapshots.find_by_fingerprint(segment_ref, candidate.fingerprint)
        if existing is not None:
            # 同输入重复固化：只对齐生效指针，绝不生成第二份快照
            window.effective_snapshot_id = existing.snapshot_id
            self._audit(
                "snapshot_dedup_skipped",
                segment_ref=segment_ref,
                window_id=window_id,
                partition=None,
                operator=operator,
                detail={"existing_snapshot_id": existing.snapshot_id,
                        "fingerprint": existing.fingerprint},
            )
            return {"snapshot_id": existing.snapshot_id, "fingerprint": existing.fingerprint,
                    "dedup": True}

        # 差异基准：优先同窗口上一版（分区重算场景）；否则取分群时间链上的上一个
        # 有效快照（新窗口首份快照），保证覆盖对象数跨窗口连续可追溯。
        same_window_previous = self.snapshots.effective_for_window(segment_ref, window_id)
        chain = self.member_chain.setdefault(segment_ref, [])
        if same_window_previous is not None:
            previous = same_window_previous
            previous_members = next(
                (members for snap_id, _, members in reversed(chain)
                 if snap_id == previous.snapshot_id),
                {},
            )
        elif chain:
            previous_snapshot_id, _, previous_members = chain[-1]
            previous = self.snapshots.get_snapshot(previous_snapshot_id)
        else:
            previous = None
            previous_members = {}

        if same_window_previous is not None:
            self.snapshots.mark_superseded(same_window_previous.snapshot_id)
            stale = snapshot_supersede_scope(
                self.snapshots,
                segment_ref=segment_ref,
                window_id=window_id,
                changed_partitions=changed_partitions,
                reason=f"窗口 {window_id} 分区 {list(changed_partitions)} 重算，快照版本追加",
            )
            self._audit(
                "diff_invalidate_scoped",
                segment_ref=segment_ref,
                window_id=window_id,
                partition=None,
                operator=operator,
                detail={"invalidated_diff_ids": [d.diff_id for d in stale],
                        "changed_partitions": list(changed_partitions)},
            )

        self.snapshots.add_snapshot(candidate)
        window.effective_snapshot_id = candidate.snapshot_id
        window.locked = True
        chain.append((candidate.snapshot_id, window_id, dict(accumulator.members)))

        # 分区重算：先按「同窗口 + 分区」范围失效旧差异，再生成新差异
        if same_window_previous is not None:
            invalidate_for_partition(
                self.snapshots,
                segment_ref=segment_ref,
                window_id=window_id,
                partition=next(iter(changed_partitions), ""),
                reason=f"分区 {list(changed_partitions)} 增量/重算，相关差异按范围失效",
            )

        diff_payload = compute_coverage_diff(
            base=previous,
            target=candidate,
            base_members=previous_members,
            target_members=dict(accumulator.members),
            affected_partitions=changed_partitions if previous is not None
            else candidate.partition_ids,
        )
        diff = CoverageDiff(
            diff_id=new_id("diff"),
            segment_ref=segment_ref,
            window_id=window_id,
            base_snapshot_id=diff_payload["base_snapshot_id"],
            target_snapshot_id=candidate.snapshot_id,
            count_before=diff_payload["count_before"],
            count_after=diff_payload["count_after"],
            added=diff_payload["added"],
            removed=diff_payload["removed"],
            profile_delta=diff_payload["profile_delta"],
            affected_partitions=diff_payload["affected_partitions"],
            status="active",
            stale_reason=None,
            created_at=utc_now(),
        )
        self.snapshots.add_diff(diff)

        self._audit(
            "snapshot_published",
            segment_ref=segment_ref,
            window_id=window_id,
            partition=None,
            operator=operator,
            detail={
                "snapshot_id": candidate.snapshot_id,
                "version": candidate.version,
                "coverage_count": candidate.coverage_count,
                "fingerprint": candidate.fingerprint,
                "diff_id": diff.diff_id,
                "partitions": list(candidate.partition_ids),
                "rule_version": candidate.rule_version,
            },
        )
        return {"snapshot_id": candidate.snapshot_id, "fingerprint": candidate.fingerprint,
                "diff_id": diff.diff_id, "dedup": False}

    # ------------------------------------------------------------- 查询视图
    def window_view(self, segment_ref: str) -> list[dict[str, Any]]:
        rule = self.rules.latest(segment_ref)
        expected = rule.expected_partitions if rule else ()
        views: list[dict[str, Any]] = []
        for window_id, window in self.windows.get(segment_ref, {}).items():
            accumulator = self.accumulators.get((segment_ref, window_id))
            views.append({
                "window_id": window_id,
                "expected_partitions": list(expected),
                "partitions": [asdict(state) for state in window.partitions.values()],
                "missing_partitions": window.missing_partitions(expected),
                "complete": window.is_complete(expected),
                "effective_snapshot_id": window.effective_snapshot_id,
                "merge_trace": [asdict(trace) for trace in accumulator.merge_trace]
                if accumulator else [],
            })
        return sorted(views, key=lambda item: item["window_id"])

    def audit_trail(
        self, *, segment_ref: str | None = None, window_id: str | None,
        action: str | None = None, limit: int = 100,
    ) -> list[dict[str, Any]]:
        events = self.audit_log
        if segment_ref is not None:
            events = [e for e in events if e.segment_ref == segment_ref]
        if window_id is not None:
            events = [e for e in events if e.window_id == window_id]
        if action is not None:
            events = [e for e in events if e.action == action]
        return [asdict(event) for event in events[-limit:]]

    def conflicts_view(self, segment_ref: str) -> list[dict[str, Any]]:
        return [asdict(c) for c in self.conflicts.get(segment_ref, [])]
