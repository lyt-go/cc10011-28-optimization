"""分群画像增量引擎接口。

挂在 /api/segmentprofile/engine 下，与既有画像定义 CRUD 互不干扰：
- 规则注册、分区批次接入、失败恢复、窗口/快照/差异/冲突查询、审计留痕；
- 所有写操作都经过引擎的分群锁与幂等判定，路由层不做业务判断。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.segment.bootstrap import get_engine
from app.segment.boundary import POLICIES
from app.segment.models import PartitionBatch, new_id

router = APIRouter(prefix="/api/segmentprofile/engine", tags=["分群画像-增量引擎"])


class SegmentRulePayload(BaseModel):
    segment_ref: str
    segment_name: str
    expression: dict[str, Any]
    profile_dimensions: list[str]
    expected_partitions: list[str]
    boundary_policy: str = "EVENT_TIME"
    operator: str = "api"


class BatchPayload(BaseModel):
    partition: str
    event_time: str
    arrival_time: str
    object_ids: list[str]
    window_id_hint: str | None = None
    operator: str = "api"


class FailPayload(BaseModel):
    partition: str
    operator: str = "api"


class RecoverPayload(BaseModel):
    batches: list[BatchPayload]
    operator: str = "api"


@router.get("/overview")
def overview() -> dict[str, Any]:
    """引擎总览：分群规则、窗口、快照与差异的计数口径。"""
    engine = get_engine()
    segments = []
    for rule in engine.rules.list_segments():
        windows = engine.window_view(rule.segment_ref)
        segments.append({
            "segment_ref": rule.segment_ref,
            "segment_name": rule.segment_name,
            "rule_version": rule.version,
            "boundary_policy": rule.boundary_policy,
            "expected_partitions": list(rule.expected_partitions),
            "window_count": len(windows),
            "complete_windows": sum(1 for w in windows if w["complete"]),
            "snapshot_count": len(engine.snapshots.list_snapshots(rule.segment_ref)),
            "diff_count": len(engine.snapshots.list_diffs(rule.segment_ref)),
        })
    return {
        "segments": segments,
        "population_size": engine.population.count(),
        "audit_event_count": len(engine.audit_log),
        "boundary_policies": list(POLICIES),
    }


@router.post("/segments")
def register_segment(payload: SegmentRulePayload) -> dict[str, Any]:
    """注册分群筛选规则；同口径重复提交幂等，不会产生第二份规则版本。"""
    engine = get_engine()
    try:
        rule = engine.register_segment(
            segment_ref=payload.segment_ref,
            segment_name=payload.segment_name,
            expression=payload.expression,
            profile_dimensions=tuple(payload.profile_dimensions),
            expected_partitions=tuple(payload.expected_partitions),
            boundary_policy=payload.boundary_policy,
            operator=payload.operator,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "rule_version": rule.version, "segment_ref": rule.segment_ref}


@router.get("/segments/{segment_ref}/windows")
def list_windows(segment_ref: str) -> dict[str, Any]:
    """窗口视图：分区接收状态、缺失分区、生效快照指针与逐步合并留痕。"""
    engine = get_engine()
    rule = engine.rules.latest(segment_ref)
    if rule is None:
        raise HTTPException(status_code=404, detail=f"分群 {segment_ref} 未注册规则")
    return {"segment_ref": segment_ref, "windows": engine.window_view(segment_ref)}


@router.post("/segments/{segment_ref}/batches")
def ingest_batch(segment_ref: str, payload: BatchPayload) -> dict[str, Any]:
    """接入一个分区批次；重复 batch 幂等，窗口到齐自动固化唯一完整快照。"""
    engine = get_engine()
    batch = PartitionBatch(
        batch_id=new_id("batch"),
        segment_ref=segment_ref,
        partition=payload.partition,
        event_time=payload.event_time,
        arrival_time=payload.arrival_time,
        object_ids=tuple(payload.object_ids),
    )
    try:
        result = engine.ingest_batch(batch, operator=payload.operator)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "result": result}


@router.post("/segments/{segment_ref}/windows/{window_id}/fail")
def mark_failed(segment_ref: str, window_id: str, payload: FailPayload) -> dict[str, Any]:
    """把分区标记失败，快照指针摘除；恢复前窗口按缺失分区处理。"""
    engine = get_engine()
    try:
        result = engine.mark_partition_failed(
            segment_ref, window_id, payload.partition, operator=payload.operator
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "result": result}


@router.post("/segments/{segment_ref}/windows/{window_id}/recover")
def recover(segment_ref: str, window_id: str, payload: RecoverPayload) -> dict[str, Any]:
    """从缺失/失败分区恢复：补提批次，补齐后自动重新固化完整快照。"""
    engine = get_engine()
    batches = [
        PartitionBatch(
            batch_id=new_id("batch"),
            segment_ref=segment_ref,
            partition=item.partition,
            event_time=item.event_time,
            arrival_time=item.arrival_time,
            object_ids=tuple(item.object_ids),
        )
        for item in payload.batches
    ]
    try:
        result = engine.recover_window(
            segment_ref, window_id, batches, operator=payload.operator
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "result": result}


@router.get("/segments/{segment_ref}/snapshots")
def list_snapshots(segment_ref: str) -> dict[str, Any]:
    """快照历史：effective 与 superseded 全部保留，历史不被覆盖。"""
    engine = get_engine()
    return {
        "segment_ref": segment_ref,
        "snapshots": [
            {
                "snapshot_id": s.snapshot_id,
                "window_id": s.window_id,
                "version": s.version,
                "status": s.status,
                "coverage_count": s.coverage_count,
                "profile": s.profile,
                "partition_ids": list(s.partition_ids),
                "batch_ids": list(s.batch_ids),
                "fingerprint": s.fingerprint,
                "rule_version": s.rule_version,
                "boundary_conflict_ids": list(s.boundary_conflict_ids),
                "created_at": s.created_at,
            }
            for s in engine.snapshots.list_snapshots(segment_ref)
        ],
    }


@router.get("/segments/{segment_ref}/diffs")
def list_diffs(segment_ref: str) -> dict[str, Any]:
    """覆盖对象与画像差异：含失效状态、失效原因和受影响分区范围。"""
    engine = get_engine()
    return {
        "segment_ref": segment_ref,
        "diffs": [
            {
                "diff_id": d.diff_id,
                "window_id": d.window_id,
                "status": d.status,
                "stale_reason": d.stale_reason,
                "base_snapshot_id": d.base_snapshot_id,
                "target_snapshot_id": d.target_snapshot_id,
                "count_before": d.count_before,
                "count_after": d.count_after,
                "added": list(d.added),
                "removed": list(d.removed),
                "profile_delta": d.profile_delta,
                "affected_partitions": list(d.affected_partitions),
                "created_at": d.created_at,
            }
            for d in engine.snapshots.list_diffs(segment_ref)
        ],
    }


@router.get("/segments/{segment_ref}/conflicts")
def list_conflicts(segment_ref: str) -> dict[str, Any]:
    """事件时间/到达时间边界冲突的裁决留痕。"""
    engine = get_engine()
    return {
        "segment_ref": segment_ref,
        "policies": list(POLICIES),
        "conflicts": [
            {
                "conflict_id": c["conflict_id"],
                "window_id": c["window_id"],
                "partition": c["partition"],
                "batch_id": c["batch_id"],
                "event_time": c["event_time"],
                "arrival_time": c["arrival_time"],
                "policy": c["policy"],
                "decision_window": c["decision_window"],
                "reason": c["reason"],
                "created_at": c["created_at"],
            }
            for c in engine.conflicts_view(segment_ref)
        ],
    }


@router.get("/audit")
def audit_trail(
    segment_ref: str | None = Query(default=None),
    window_id: str | None = Query(default=None),
    action: str | None = Query(default=None),
    limit: int = Query(default=100, le=500),
) -> dict[str, Any]:
    """审计轨迹：所有关键动作只增不改，可按分群/窗口/动作过滤。"""
    engine = get_engine()
    return {
        "events": engine.audit_trail(
            segment_ref=segment_ref, window_id=window_id, action=action, limit=limit
        )
    }
