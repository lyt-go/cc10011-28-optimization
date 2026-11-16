"""分群画像增量合并 —— 全项目唯一的合并实现（可审计核心）。

设计约束（对应「增量合并需要抽取独立实现并写成一份可审计逻辑，避免不同分区
各写了一遍」）：

1. 这里是合并口径的唯一入口。任何分区（无论按事件时间还是到达时间切分）都走
   同一个 ``MergeAccumulator.add_partition``，代码里不存在按分区名分支的合并逻辑。
2. 合并过程全部留痕：每个分区贡献了多少对象、过滤掉多少未知 ID、每次合并的
   批次与时间戳，都进 ``merge_trace``，可逐条审计、可按相同输入重放。
3. 合并不直接落库：输入是内存累加器，输出是不可变快照对象，由编排引擎在分群锁内
   决定是否生效，保证并发到达时一个窗口只有一个完整快照。
4. 纯函数化的 ``build_snapshot``：同样的累加状态 + 规则版本必然得到同 fingerprint
   的快照，重复提交因此天然幂等。
"""
from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, field
from typing import Any

from app.segment.models import (
    PartitionBatch,
    PopulationRecord,
    SegmentRule,
    Snapshot,
    new_id,
    utc_now,
)
from app.segment.rule_eval import evaluate_expression


@dataclass
class MergeTraceEntry:
    """单分区合并的审计留痕。"""

    step: str
    batch_id: str
    partition: str
    incoming_count: int
    unknown_ids: list[str]
    matched_count: int
    new_object_ids: list[str]
    duplicate_object_ids: list[str]
    merged_at: str


@dataclass
class MergeAccumulator:
    """窗口级增量累加器。所有分区共用这一份实现，状态与分区名称解耦。"""

    segment_ref: str
    window_id: str
    rule_version: int
    dimensions: tuple[str, ...]
    # 已纳入窗口的对象：object_id -> 来源分区（去重依据）
    members: dict[str, str] = field(default_factory=dict)
    # 每个分区已合并的批次，幂等去重
    merged_batches: dict[str, str] = field(default_factory=dict)
    # 分区 -> 画像维度取值的聚合中间态
    profile_by_partition: dict[str, dict[str, dict[str, int]]] = field(default_factory=dict)
    merge_trace: list[MergeTraceEntry] = field(default_factory=list)
    # 用 RLock：replace_partition 持锁后复用 add_partition，同一线程可重入
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def add_partition(
        self,
        batch: PartitionBatch,
        records: list[PopulationRecord],
        rule: SegmentRule,
    ) -> MergeTraceEntry:
        """把一个分区批次并入窗口。重复批次返回既有留痕，不产生第二次合并。"""
        with self._lock:
            previous = self.merged_batches.get(batch.partition)
            if previous is not None:
                if previous == batch.batch_id:
                    # 同一批次重复提交：幂等返回，不改变任何状态
                    return next(
                        trace for trace in self.merge_trace
                        if trace.batch_id == batch.batch_id
                    )
                raise ValueError(
                    f"分区 {batch.partition} 已合并批次 {previous}，"
                    f"新批次 {batch.batch_id} 必须走分区重算/恢复流程，不能直接覆盖"
                )

            known_ids = {record.object_id for record in records}
            unknown_ids = [oid for oid in batch.object_ids if oid not in known_ids]

            new_object_ids: list[str] = []
            duplicate_object_ids: list[str] = []
            dimension_bucket: dict[str, dict[str, int]] = {
                dim: {} for dim in self.dimensions
            }

            for record in records:
                if not evaluate_expression(record, rule.expression):
                    continue
                if record.object_id in self.members:
                    duplicate_object_ids.append(record.object_id)
                    continue
                self.members[record.object_id] = batch.partition
                new_object_ids.append(record.object_id)
                for dim in self.dimensions:
                    value = record.get(dim) or "未知"
                    bucket = dimension_bucket.setdefault(dim, {})
                    bucket[value] = bucket.get(value, 0) + 1

            self.profile_by_partition[batch.partition] = dimension_bucket
            self.merged_batches[batch.partition] = batch.batch_id

            trace = MergeTraceEntry(
                step="partition_merge",
                batch_id=batch.batch_id,
                partition=batch.partition,
                incoming_count=len(batch.object_ids),
                unknown_ids=unknown_ids,
                matched_count=len(new_object_ids),
                new_object_ids=new_object_ids,
                duplicate_object_ids=duplicate_object_ids,
                merged_at=utc_now(),
            )
            self.merge_trace.append(trace)
            return trace

    def replace_partition(
        self,
        batch: PartitionBatch,
        records: list[PopulationRecord],
        rule: SegmentRule,
    ) -> MergeTraceEntry:
        """分区重算/恢复入口：先摘除该分区旧贡献，再走同一个 add_partition。

        这仍然是合并的唯一实现——重算不是另写一遍合并，而是回滚分区贡献后
        复用 add_partition，保证两种路径的口径完全一致。
        """
        with self._lock:
            old_partition_members = [
                object_id for object_id, source in self.members.items()
                if source == batch.partition
            ]
            for object_id in old_partition_members:
                self.members.pop(object_id, None)
            self.profile_by_partition.pop(batch.partition, None)
            self.merged_batches.pop(batch.partition, None)
            self.merge_trace.append(MergeTraceEntry(
                step="partition_rollback",
                batch_id=batch.batch_id,
                partition=batch.partition,
                incoming_count=len(old_partition_members),
                unknown_ids=[],
                matched_count=0,
                new_object_ids=[],
                duplicate_object_ids=[],
                merged_at=utc_now(),
            ))
            return self.add_partition(batch, records, rule)

    def aggregate_profile(self) -> dict[str, dict[str, int]]:
        """把各分区的维度聚合中间态合并成完整窗口画像（不区分分区来源）。"""
        merged: dict[str, dict[str, int]] = {dim: {} for dim in self.dimensions}
        for bucket in self.profile_by_partition.values():
            for dim, counts in bucket.items():
                target = merged.setdefault(dim, {})
                for value, count in counts.items():
                    target[value] = target.get(value, 0) + count
        return merged

    def fingerprint_payload(self, boundary_conflict_ids: tuple[str, ...]) -> str:
        """参与指纹的完整口径：成员集合 + 画像 + 分区批次序列 + 规则版本。"""
        payload = {
            "segment_ref": self.segment_ref,
            "window_id": self.window_id,
            "rule_version": self.rule_version,
            "members": sorted(self.members),
            "partitions": dict(sorted(self.merged_batches.items())),
            "profile": self.aggregate_profile(),
            "boundary_conflicts": sorted(boundary_conflict_ids),
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def new_accumulator(rule: SegmentRule, window_id: str) -> MergeAccumulator:
    """构造窗口累加器。所有分区都从这里开始，不存在各分区各写一份初始化。"""
    return MergeAccumulator(
        segment_ref=rule.segment_ref,
        window_id=window_id,
        rule_version=rule.version,
        dimensions=rule.profile_dimensions,
    )


def build_snapshot(
    accumulator: MergeAccumulator,
    *,
    version: int,
    boundary_conflict_ids: tuple[str, ...] = (),
) -> Snapshot:
    """把累加器固化为不可变快照。同输入同指纹，是重复提交幂等的判定依据。"""
    profile = accumulator.aggregate_profile()
    fingerprint = hashlib.sha256(
        accumulator.fingerprint_payload(boundary_conflict_ids).encode("utf-8")
    ).hexdigest()
    return Snapshot(
        snapshot_id=new_id("snap"),
        segment_ref=accumulator.segment_ref,
        window_id=accumulator.window_id,
        version=version,
        coverage_count=len(accumulator.members),
        profile=profile,
        partition_ids=tuple(sorted(accumulator.merged_batches)),
        batch_ids=tuple(accumulator.merged_batches[partition]
                        for partition in sorted(accumulator.merged_batches)),
        fingerprint=fingerprint,
        rule_version=accumulator.rule_version,
        boundary_conflict_ids=tuple(sorted(boundary_conflict_ids)),
        status="effective",
        created_at=utc_now(),
    )


def compute_coverage_diff(
    *,
    base: Snapshot | None,
    target: Snapshot,
    base_members: dict[str, str],
    target_members: dict[str, str],
    affected_partitions: tuple[str, ...],
) -> dict[str, Any]:
    """计算两个相邻快照的覆盖对象差异与画像维度差异。

    成员集合（object_id -> 来源分区）由编排引擎从合并累加器状态传入，快照本体只
    存指纹与计数，避免大字段冗余；没有历史基准（首个快照）时 count_before 记 0、
    added 为全量，仍生成差异记录，保证覆盖对象数与画像差异从第一版起就可追溯。
    """
    base_ids = set(base_members)
    target_ids = set(target_members)
    if base is None:
        added = tuple(sorted(target_ids))
        removed = tuple()
        count_before = 0
        base_id = ""
    else:
        added = tuple(sorted(target_ids - base_ids))
        removed = tuple(sorted(base_ids - target_ids))
        count_before = base.coverage_count
        base_id = base.snapshot_id

    profile_delta: dict[str, dict[str, int]] = {}
    dimensions = set(target.profile) | (set(base.profile) if base else set())
    for dim in sorted(dimensions):
        before = (base.profile.get(dim, {}) if base else {})
        after = target.profile.get(dim, {})
        values = set(before) | set(after)
        delta = {value: after.get(value, 0) - before.get(value, 0) for value in sorted(values)}
        profile_delta[dim] = {key: value for key, value in delta.items() if value != 0}

    return {
        "count_before": count_before,
        "count_after": target.coverage_count,
        "added": added,
        "removed": removed,
        "profile_delta": profile_delta,
        "base_snapshot_id": base_id,
        "affected_partitions": tuple(sorted(set(affected_partitions))),
    }
