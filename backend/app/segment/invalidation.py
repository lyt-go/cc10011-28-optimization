"""画像差异的失效范围控制。

解决的问题：旧口径下覆盖对象数稍有变化就把所有画像差异标过期。
新口径：每条差异记录它受哪些分区影响（affected_partitions），只有当这些分区
发生重算/恢复时，差异才允许过期；其它分区的增量到达不得动这条差异。
"""
from __future__ import annotations

from app.segment.models import AuditEvent, CoverageDiff
from app.segment.repository import SnapshotRepository


def invalidate_for_partition(
    repo: SnapshotRepository,
    *,
    segment_ref: str,
    window_id: str,
    partition: str,
    reason: str,
) -> list[CoverageDiff]:
    """把指定窗口内与指定分区相关的活动差异标为 stale，返回被失效的差异列表。

    注意：
    - 只改状态，不删除记录；不相关分区、其它窗口的差异保持 active；
    - 窗口隔离是硬约束：A 窗口分区重算不得失效 B 窗口的画像差异。
    """
    stale: list[CoverageDiff] = []
    for diff in repo.diffs_touching(segment_ref, partition, window_id=window_id):
        diff.status = "stale"
        diff.stale_reason = reason
        stale.append(diff)
    return stale


def snapshot_supersede_scope(
    repo: SnapshotRepository,
    *,
    segment_ref: str,
    window_id: str,
    changed_partitions: tuple[str, ...],
    reason: str,
) -> list[CoverageDiff]:
    """快照被新版本取代时的差异失效：限定同窗口、限定变化分区范围。"""
    changed = set(changed_partitions)
    stale: list[CoverageDiff] = []
    for diff in repo.list_diffs(segment_ref):
        if diff.status != "active" or diff.window_id != window_id:
            continue
        if diff.affected_partitions and not changed.intersection(diff.affected_partitions):
            continue
        diff.status = "stale"
        diff.stale_reason = reason
        stale.append(diff)
    return stale


def describe_invalidation(stale_diffs: list[CoverageDiff], partition: str) -> AuditEvent:
    """失效动作本身也要可审计。"""
    from app.segment.models import new_id, utc_now

    return AuditEvent(
        event_id=new_id("audit"),
        created_at=utc_now(),
        action="diff_invalidate_scoped",
        segment_ref=stale_diffs[0].segment_ref if stale_diffs else None,
        window_id=stale_diffs[0].window_id if stale_diffs else None,
        partition=partition,
        operator="engine",
        detail={
            "invalidated_diff_ids": [diff.diff_id for diff in stale_diffs],
            "count": len(stale_diffs),
        },
    )
