"""共享数据仓库：目标人群数据与分群筛选规则。

职责边界（对应「共享的目标人群数据与分群筛选规则只允许职责内处理」）：
- 目标人群数据只通过受控入口写入，引擎与下游只能读，不能改；
- 分群筛选规则只能注册新版本，不能就地改写历史版本；
- 注册按 segment_ref 幂等：重复提交同口径规则不会产生第二份规则版本。
"""
from __future__ import annotations

from typing import Any

from app.segment.models import (
    CoverageDiff,
    PopulationRecord,
    SegmentRule,
    Snapshot,
    utc_now,
)


class PopulationRepository:
    def __init__(self) -> None:
        self._records: dict[str, PopulationRecord] = {}

    def load(self, records: list[PopulationRecord]) -> None:
        """初始化装载（仅演示/恢复场景使用），按 object_id 去重。"""
        for record in records:
            self._records[record.object_id] = record

    def get_many(self, object_ids: list[str] | tuple[str, ...]) -> list[PopulationRecord]:
        """按 ID 批量读取；不存在的 ID 会被显式跳过并由调用方记审计。"""
        return [self._records[oid] for oid in object_ids if oid in self._records]

    def get(self, object_id: str) -> PopulationRecord | None:
        return self._records.get(object_id)

    def count(self) -> int:
        return len(self._records)


class RuleRegistry:
    def __init__(self) -> None:
        self._rules: dict[str, list[SegmentRule]] = {}

    def register(
        self,
        *,
        segment_ref: str,
        segment_name: str,
        expression: dict[str, Any],
        profile_dimensions: tuple[str, ...],
        expected_partitions: tuple[str, ...],
        boundary_policy: str,
    ) -> SegmentRule:
        """注册规则。同口径重复提交返回当前版本（幂等），不产生新版本。"""
        versions = self._rules.setdefault(segment_ref, [])
        for existing in versions:
            if (
                existing.expression == expression
                and existing.profile_dimensions == tuple(profile_dimensions)
                and existing.expected_partitions == tuple(expected_partitions)
                and existing.boundary_policy == boundary_policy
            ):
                return existing
        rule = SegmentRule(
            segment_ref=segment_ref,
            segment_name=segment_name,
            expression=dict(expression),
            profile_dimensions=tuple(profile_dimensions),
            expected_partitions=tuple(expected_partitions),
            boundary_policy=boundary_policy,
            version=len(versions) + 1,
            registered_at=utc_now(),
        )
        versions.append(rule)
        return rule

    def latest(self, segment_ref: str) -> SegmentRule | None:
        versions = self._rules.get(segment_ref)
        return versions[-1] if versions else None

    def get_version(self, segment_ref: str, version: int) -> SegmentRule | None:
        for rule in self._rules.get(segment_ref, []):
            if rule.version == version:
                return rule
        return None

    def list_segments(self) -> list[SegmentRule]:
        return [versions[-1] for versions in self._rules.values()]


class SnapshotRepository:
    """画像快照与差异的不可变存储。只追加、不覆盖。"""

    def __init__(self) -> None:
        self._snapshots: dict[str, list[Snapshot]] = {}
        self._diffs: dict[str, list[CoverageDiff]] = {}
        self._by_id: dict[str, Snapshot] = {}

    def add_snapshot(self, snapshot: Snapshot) -> None:
        self._snapshots.setdefault(snapshot.segment_ref, []).append(snapshot)
        self._by_id[snapshot.snapshot_id] = snapshot

    def get_snapshot(self, snapshot_id: str) -> Snapshot | None:
        return self._by_id.get(snapshot_id)

    def list_snapshots(self, segment_ref: str) -> list[Snapshot]:
        return list(self._snapshots.get(segment_ref, []))

    def find_by_fingerprint(self, segment_ref: str, fingerprint: str) -> Snapshot | None:
        for snapshot in self._snapshots.get(segment_ref, []):
            if snapshot.fingerprint == fingerprint:
                return snapshot
        return None

    def effective_for_window(self, segment_ref: str, window_id: str) -> Snapshot | None:
        for snapshot in reversed(self._snapshots.get(segment_ref, [])):
            if snapshot.window_id == window_id and snapshot.status == "effective":
                return snapshot
        return None

    def mark_superseded(self, snapshot_id: str) -> None:
        """把旧快照标为 superseded：记录保留、不可删除，历史仍可追溯。"""
        snapshot = self._by_id.get(snapshot_id)
        if snapshot is not None:
            object.__setattr__(snapshot, "status", "superseded")

    def add_diff(self, diff: CoverageDiff) -> None:
        self._diffs.setdefault(diff.segment_ref, []).append(diff)

    def list_diffs(self, segment_ref: str) -> list[CoverageDiff]:
        return list(self._diffs.get(segment_ref, []))

    def diffs_touching(
        self, segment_ref: str, partition: str, window_id: str | None = None
    ) -> list[CoverageDiff]:
        """活动差异中受指定分区影响的记录；可再限定窗口，防止失效范围越窗。"""
        return [
            diff for diff in self._diffs.get(segment_ref, [])
            if partition in diff.affected_partitions
            and diff.status == "active"
            and (window_id is None or diff.window_id == window_id)
        ]
