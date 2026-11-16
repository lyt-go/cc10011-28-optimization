"""分群画像增量引擎的数据模型。

全部为不可冻结/半冻结的数据载体，业务判断不放在这里：
- PopulationRecord：共享目标人群数据（引擎只读）
- SegmentRule：分群筛选规则（受控注册，带版本）
- PartitionBatch：分区到达批次（增量输入）
- Snapshot：画像快照（一旦生成不可变，只允许追加新版本）
- CoverageDiff：覆盖对象差异与画像差异（可按分区失效）
- BoundaryConflict：事件时间/到达时间边界冲突裁决记录
- AuditEvent：审计事件
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> str:
    """统一时间戳口径，避免各模块各写一遍 datetime.now。"""
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True)
class PopulationRecord:
    """目标人群中的一个对象。event_date 为事件时间所属自然日，region 为分区键。"""

    object_id: str
    event_date: str
    region: str
    attributes: dict[str, str]

    def get(self, key: str) -> str | None:
        if key == "region":
            return self.region
        if key == "event_date":
            return self.event_date
        return self.attributes.get(key)


@dataclass(frozen=True)
class SegmentRule:
    """分群筛选规则。

    expression 为结构化表达式：
      条件节点 {"field": "level", "op": "eq", "value": "高"}
      组合节点 {"logic": "and" | "or", "conditions": [...]}
    """

    segment_ref: str
    segment_name: str
    expression: dict[str, Any]
    profile_dimensions: tuple[str, ...]
    expected_partitions: tuple[str, ...]
    boundary_policy: str  # EVENT_TIME / ARRIVAL_TIME / ARRIVAL_FIRST_LOCK
    version: int
    registered_at: str


@dataclass(frozen=True)
class PartitionBatch:
    """一个分群某个窗口的一个分区数据批次，增量更新的最小输入单元。"""

    batch_id: str
    segment_ref: str
    partition: str
    event_time: str
    arrival_time: str
    object_ids: tuple[str, ...]


@dataclass
class PartitionState:
    """窗口内单个分区的接收状态。失败分区可通过重新提交恢复。"""

    window_id: str
    partition: str
    status: str  # received / failed
    batch_id: str
    object_count: int
    event_time: str
    arrival_time: str
    attempts: int = 1


@dataclass
class WindowState:
    """分群-窗口维度的运行态：分区接收情况、批次序列、生效快照。"""

    segment_ref: str
    window_id: str
    partitions: dict[str, PartitionState] = field(default_factory=dict)
    batch_ids: list[str] = field(default_factory=list)
    locked: bool = False
    effective_snapshot_id: str | None = None

    def missing_partitions(self, expected: tuple[str, ...]) -> list[str]:
        return [
            p for p in expected
            if p not in self.partitions or self.partitions[p].status == "failed"
        ]

    def is_complete(self, expected: tuple[str, ...]) -> bool:
        return not self.missing_partitions(expected)


@dataclass(frozen=True)
class Snapshot:
    """完整画像快照。immutable：历史快照永不被覆盖，只追加新版本。"""

    snapshot_id: str
    segment_ref: str
    window_id: str
    version: int
    coverage_count: int
    profile: dict[str, dict[str, int]]
    partition_ids: tuple[str, ...]
    batch_ids: tuple[str, ...]
    fingerprint: str
    rule_version: int
    boundary_conflict_ids: tuple[str, ...]
    status: str  # effective / superseded
    created_at: str


@dataclass
class CoverageDiff:
    """两个相邻有效快照之间的覆盖对象差异与画像维度差异。

    affected_partitions 是差异的失效边界：只有这些分区重算时，本差异才允许过期，
    其它分区变化不得把本差异标过期。
    """

    diff_id: str
    segment_ref: str
    window_id: str
    base_snapshot_id: str
    target_snapshot_id: str
    count_before: int
    count_after: int
    added: tuple[str, ...]
    removed: tuple[str, ...]
    profile_delta: dict[str, dict[str, int]]
    affected_partitions: tuple[str, ...]
    status: str  # active / stale
    stale_reason: str | None
    created_at: str


@dataclass(frozen=True)
class BoundaryConflict:
    """事件时间与数据到达时间跨日冲突的裁决留痕。"""

    conflict_id: str
    segment_ref: str
    window_id: str
    partition: str
    batch_id: str
    event_time: str
    arrival_time: str
    policy: str
    decision_window: str
    reason: str
    created_at: str


@dataclass(frozen=True)
class AuditEvent:
    """审计事件，所有关键动作都追加一条，只增不改。"""

    event_id: str
    created_at: str
    action: str
    segment_ref: str | None
    window_id: str | None
    partition: str | None
    operator: str
    detail: dict[str, Any]
