"""分群分区的事件时间 / 数据到达时间边界冲突裁决。

同一条增量批次同时携带 event_time（业务事件发生日）与 arrival_time（数据到达日）。
当两者落到不同窗口（跨日）时，必须保证「覆盖对象数只对应一个画像快照」——
即一个批次只能归入一个窗口，不允许两边各算一遍。

三种可选策略，按业务代价权衡：

- EVENT_TIME（事件时间归窗，默认）
  口径：始终按 event_time 归窗。业务语义最准确，跨日到达的数据补进历史窗口，
  历史窗口会产生新快照版本（旧快照保留不覆盖）。
  代价：历史窗口可能被迟到数据反复修正，下游需容忍版本追加。

- ARRIVAL_TIME（到达时间归窗）
  口径：按 arrival_time 归窗，窗口一旦定稿不再变化。
  代价：实现最简单、下游最稳定，但跨日事件被记到到达日，业务口径有固定偏差。

- ARRIVAL_FIRST_LOCK（首达锁定）
  口径：同一分区第一次到达用哪个窗口，后续重算/补数都锁定该窗口。
  代价：兼顾稳定性与首达语义，但需要维护分区-窗口锁定关系，迟到且口径不一致的
  数据会被拒绝并要求人工裁决。
"""
from __future__ import annotations

from app.segment.models import (
    BoundaryConflict,
    PartitionBatch,
    SegmentRule,
    WindowState,
    new_id,
    utc_now,
)

POLICIES = ("EVENT_TIME", "ARRIVAL_TIME", "ARRIVAL_FIRST_LOCK")
DEFAULT_POLICY = "EVENT_TIME"


def _day_of(timestamp: str) -> str:
    """取 ISO 时间戳的自然日部分（YYYY-MM-DD），窗口按天划分。"""
    return timestamp[:10]


def resolve_window(
    batch: PartitionBatch,
    rule: SegmentRule,
    windows: dict[str, WindowState],
) -> tuple[str, BoundaryConflict | None]:
    """裁决批次归属窗口。返回 (window_id, 冲突记录或 None)。

    不变量：无论是否冲突，一个批次只返回一个 window_id，覆盖对象数因此只可能
    进入一个窗口的一个完整快照。

    ARRIVAL_FIRST_LOCK 的首达语义只在「同窗口内该分区已有接收记录」时生效：
    跨日冲突下，分区首次到达落在哪个窗口，后续补数必须回到同一窗口；新窗口的
    正常批次不受历史窗口影响。
    """
    policy = rule.boundary_policy if rule.boundary_policy in POLICIES else DEFAULT_POLICY
    event_day = _day_of(batch.event_time)
    arrival_day = _day_of(batch.arrival_time)

    if policy == "EVENT_TIME":
        decision = event_day
    elif policy == "ARRIVAL_TIME":
        decision = arrival_day
    else:  # ARRIVAL_FIRST_LOCK
        arrival_window = windows.get(arrival_day)
        if arrival_window is not None and batch.partition in arrival_window.partitions:
            decision = arrival_day
        else:
            decision = event_day

    conflict = None
    if event_day != arrival_day:
        reason = _reason_text(policy, event_day, arrival_day)
        conflict = BoundaryConflict(
            conflict_id=new_id("conf"),
            segment_ref=batch.segment_ref,
            window_id=decision,
            partition=batch.partition,
            batch_id=batch.batch_id,
            event_time=batch.event_time,
            arrival_time=batch.arrival_time,
            policy=policy,
            decision_window=decision,
            reason=reason,
            created_at=utc_now(),
        )
    return decision, conflict


def _reason_text(policy: str, event_day: str, arrival_day: str) -> str:
    if policy == "EVENT_TIME":
        return (
            f"事件时间 {event_day} 与到达时间 {arrival_day} 跨日；"
            "按 EVENT_TIME 归入事件日窗口，历史窗口追加快照版本，旧版本保留可追溯"
        )
    if policy == "ARRIVAL_TIME":
        return (
            f"事件时间 {event_day} 与到达时间 {arrival_day} 跨日；"
            "按 ARRIVAL_TIME 归入到达日窗口，窗口定稿后不再修正，接受固定口径偏差"
        )
    return (
        f"事件时间 {event_day} 与到达时间 {arrival_day} 跨日；"
        "按 ARRIVAL_FIRST_LOCK 采用首达窗口，后续同分区补数必须回到同一窗口"
    )
