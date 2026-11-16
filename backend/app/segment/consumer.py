"""事件分析对分群画像快照的同步消费器。

职责边界（对应「另一个业务模块事件分析同步消费分群画像快照」）：
- 只读：消费器不写引擎、不写快照仓库，只记录「消费了哪个快照」；
- 跟着现有调用走：下游步骤从 eventanalysis 服务里已启用（已启用状态）的分析模型
  解析，保证消费链路与现有模块的调用定位一致；
- 只消费 effective 快照；快照被 supersede 后，下次同步自动切到新版本并留痕，
  历史消费记录保留，可追溯当时用的是哪一份快照、覆盖对象数是多少。
"""
from __future__ import annotations

from dataclasses import asdict

from app.segment.engine import SegmentProfileEngine
from app.segment.models import new_id, utc_now
from app.segment.repository import SnapshotRepository
from app.store import store

EVENTANALYSIS_MODULE = "eventanalysis"
ENABLED_STATUS = "已启用"


class SnapshotConsumer:
    def __init__(self, engine: SegmentProfileEngine, snapshots: SnapshotRepository) -> None:
        self.engine = engine
        self.snapshots = snapshots
        # segment_ref -> 消费记录列表（只追加）
        self.consumptions: dict[str, list[dict]] = {}

    def downstream_steps(self) -> list[dict[str, str]]:
        """跟随现有 eventanalysis 调用：取已启用模型作为下游分析步骤。"""
        rows = store.rows(EVENTANALYSIS_MODULE)
        return [
            {
                "model_id": str(row.get("模型编号", "")),
                "event_name": str(row.get("事件名称", "")),
                "group_dimension": str(row.get("分组维度", "")),
                "status": str(row.get("status", "")),
            }
            for row in rows
            if row.get("status") == ENABLED_STATUS
        ]

    def consume_latest(self, segment_ref: str, *, operator: str = "eventanalysis") -> dict:
        """同步消费分群当前所有窗口的 effective 快照。"""
        steps = self.downstream_steps()
        records = self.consumptions.setdefault(segment_ref, [])
        synced: list[dict] = []
        for window in self.engine.window_view(segment_ref):
            snapshot_id = window["effective_snapshot_id"]
            if not snapshot_id:
                continue
            snapshot = self.snapshots.get_snapshot(snapshot_id)
            if snapshot is None or snapshot.status != "effective":
                continue
            record = {
                "consumption_id": new_id("cons"),
                "segment_ref": segment_ref,
                "window_id": snapshot.window_id,
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_version": snapshot.version,
                "coverage_count": snapshot.coverage_count,
                "fingerprint": snapshot.fingerprint,
                "downstream_step_count": len(steps),
                "downstream_steps": steps,
                "consumed_at": utc_now(),
                "operator": operator,
            }
            records.append(record)
            synced.append(record)
            self.engine._audit(  # noqa: SLF001 - 审计入口由引擎统一收口
                "snapshot_consumed",
                segment_ref=segment_ref,
                window_id=snapshot.window_id,
                partition=None,
                operator=operator,
                detail={
                    "snapshot_id": snapshot.snapshot_id,
                    "coverage_count": snapshot.coverage_count,
                    "downstream_models": [step["model_id"] for step in steps],
                },
            )
        return {"segment_ref": segment_ref, "synced": synced, "downstream_steps": steps}

    def consumption_history(self, segment_ref: str) -> list[dict]:
        return list(self.consumptions.get(segment_ref, []))
