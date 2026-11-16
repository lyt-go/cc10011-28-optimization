"""分群筛选规则的结构化表达式求值。

只依赖标准库、不依赖任何仓库状态，保证规则口径在过滤与重放时完全一致。
支持操作：eq / ne / in / not_in / contains；组合逻辑：and / or。
"""
from __future__ import annotations

from typing import Any

from app.segment.models import PopulationRecord

_ALLOWED_OPS = {"eq", "ne", "in", "not_in", "contains"}


def evaluate_expression(record: PopulationRecord, expression: dict[str, Any]) -> bool:
    """对单条人群对象求值筛选表达式。"""
    if "logic" in expression:
        logic = expression["logic"]
        conditions = expression.get("conditions", [])
        if logic == "and":
            return all(evaluate_expression(record, cond) for cond in conditions)
        if logic == "or":
            return any(evaluate_expression(record, cond) for cond in conditions)
        raise ValueError(f"不支持的组合逻辑：{logic}，仅允许 and / or")

    op = expression.get("op")
    if op not in _ALLOWED_OPS:
        raise ValueError(f"不支持的比较操作：{op}，允许：{sorted(_ALLOWED_OPS)}")
    field_name = expression["field"]
    expected = expression.get("value")
    actual = record.get(field_name)

    if op == "eq":
        return actual == expected
    if op == "ne":
        return actual != expected
    if op == "in":
        return actual in (expected or [])
    if op == "not_in":
        return actual not in (expected or [])
    if op == "contains":
        return expected is not None and expected in (actual or "")
    return False  # 不可达，留作防御


def parse_text_rule(text: str) -> dict[str, Any] | None:
    """把旧数据里的纯文本筛选规则解析成结构化表达式。

    解析不出来时返回 None，由注册方决定口径（默认不额外过滤），
    并在审计事件中保留原文，避免静默改变筛选口径。
    """
    raw = (text or "").strip()
    if not raw:
        return None
    # 形如  level=高 或 level eq 高
    for op_token, op in (("=", "eq"), (" eq ", "eq")):
        if op_token in raw:
            key, value = raw.split(op_token, 1)
            key = key.strip()
            value = value.strip()
            if key and value:
                return {"field": key, "op": op, "value": value}
    return None
