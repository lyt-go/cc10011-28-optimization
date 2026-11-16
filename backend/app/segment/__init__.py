"""分群画像增量引擎领域包。

模块职责一览：
- models      数据模型（快照、差异、批次、冲突、审计）
- rule_eval   筛选规则结构化求值（纯函数）
- repository  目标人群/规则/快照的受控存储
- merge       全项目唯一的增量合并实现（可审计）
- invalidation 画像差异的分区级失效范围
- boundary    事件时间/到达时间边界冲突裁决
- engine      分群锁编排：幂等接入、失败恢复、快照唯一性
- consumer    事件分析下游的只读快照消费器
- bootstrap   装配与演示数据引导
"""
from __future__ import annotations

from app.segment.bootstrap import get_consumer, get_engine, get_population

__all__ = ["get_engine", "get_consumer", "get_population"]
