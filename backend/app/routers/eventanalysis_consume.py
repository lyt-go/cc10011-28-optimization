"""事件分析对分群画像快照的同步消费接口。

独立路由文件挂到 eventanalysis 前缀下：消费动作属于事件分析模块的职责，
但只通过 SnapshotConsumer 只读访问分群快照，不回写分群引擎。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.segment.bootstrap import get_consumer, get_engine

router = APIRouter(prefix="/api/eventanalysis", tags=["事件分析-快照消费"])


class ConsumePayload(BaseModel):
    segment_ref: str
    operator: str = "eventanalysis"


@router.get("/snapshot-consumption/downstream-steps")
def downstream_steps() -> dict[str, object]:
    """跟随现有调用：返回当前已启用的事件分析模型，即消费快照后的下游步骤。"""
    consumer = get_consumer()
    steps = consumer.downstream_steps()
    return {"steps": steps, "total": len(steps)}


@router.post("/snapshot-consumption/sync")
def sync_consumption(payload: ConsumePayload) -> dict[str, object]:
    """同步消费分群各窗口当前 effective 的画像快照并留痕。"""
    engine = get_engine()
    if engine.rules.latest(payload.segment_ref) is None:
        raise HTTPException(status_code=404, detail=f"分群 {payload.segment_ref} 未注册规则")
    consumer = get_consumer()
    return consumer.consume_latest(payload.segment_ref, operator=payload.operator)


@router.get("/snapshot-consumption/{segment_ref}/history")
def consumption_history(segment_ref: str) -> dict[str, object]:
    """消费历史：每次同步用的快照版本与覆盖对象数都可追溯。"""
    consumer = get_consumer()
    history = consumer.consumption_history(segment_ref)
    return {"segment_ref": segment_ref, "total": len(history), "history": history}
