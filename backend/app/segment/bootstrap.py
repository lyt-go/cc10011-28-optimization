"""分群画像增量引擎的装配与演示数据引导。

- build_engine() 组装共享仓库、规则注册表、快照仓库与引擎单例；
- seed_demo_data() 预置目标人群、两个分群规则与若干分区批次，覆盖：
  完整窗口、缺失分区、跨日边界冲突、重复提交、失败恢复五类场景；
- 引导过程只通过引擎公开 API 写数据，不绕过任何锁与审计。
"""
from __future__ import annotations

from app.segment.boundary import DEFAULT_POLICY
from app.segment.consumer import SnapshotConsumer
from app.segment.engine import SegmentProfileEngine
from app.segment.models import PartitionBatch, PopulationRecord, new_id, utc_now
from app.segment.repository import PopulationRepository, RuleRegistry, SnapshotRepository

REGIONS = ("east", "west")
DATES = ("2026-11-10", "2026-11-11", "2026-11-12")
LEVELS = ("高", "中", "低")
CHANNELS = ("app", "web", "mini")


def _demo_population() -> list[PopulationRecord]:
    records: list[PopulationRecord] = []
    for date_index, date in enumerate(DATES):
        for region_index, region in enumerate(REGIONS):
            for seq in range(4):
                object_id = f"u-{date[-2:]}-{region[0]}-{seq}"
                # 让属性分布随日期/分区变化，增量差异才看得出来
                level = LEVELS[(seq + date_index + region_index) % len(LEVELS)]
                channel = CHANNELS[(seq + region_index) % len(CHANNELS)]
                records.append(PopulationRecord(
                    object_id=object_id,
                    event_date=date,
                    region=region,
                    attributes={"level": level, "channel": channel, "name": f"用户{object_id}"},
                ))
    return records


def _batch_for(
    segment_ref: str, population: list[PopulationRecord],
    *, date: str, region: str, arrival_day: str | None = None,
) -> PartitionBatch:
    object_ids = tuple(
        record.object_id for record in population
        if record.event_date == date and record.region == region
    )
    arrival = f"{arrival_day or date}T08:30:00+00:00"
    return PartitionBatch(
        batch_id=new_id("batch"),
        segment_ref=segment_ref,
        partition=region,
        event_time=f"{date}T02:00:00+00:00",
        arrival_time=arrival,
        object_ids=object_ids,
    )


def build_engine() -> tuple[SegmentProfileEngine, SnapshotConsumer, PopulationRepository]:
    population = PopulationRepository()
    rules = RuleRegistry()
    snapshots = SnapshotRepository()
    engine = SegmentProfileEngine(population, rules, snapshots)
    consumer = SnapshotConsumer(engine, snapshots)
    return engine, consumer, population


def seed_demo_data(engine: SegmentProfileEngine, population: PopulationRepository) -> dict[str, object]:
    population.load(_demo_population())
    all_records = _demo_population()

    # 分群一：高价值用户，按事件时间归窗，画像维度 level/channel
    seg_high = "SEGM-0003"
    engine.register_segment(
        segment_ref=seg_high,
        segment_name="高价值用户分群",
        expression={"field": "level", "op": "eq", "value": "高"},
        profile_dimensions=("level", "channel"),
        expected_partitions=REGIONS,
        boundary_policy=DEFAULT_POLICY,
    )

    # 分群二：app 渠道用户，首达锁定策略
    seg_app = "SEGM-0002"
    engine.register_segment(
        segment_ref=seg_app,
        segment_name="App渠道分群",
        expression={"field": "channel", "op": "eq", "value": "app"},
        profile_dimensions=("channel", "level"),
        expected_partitions=REGIONS,
        boundary_policy="ARRIVAL_FIRST_LOCK",
    )

    summary: dict[str, object] = {"population": population.count(), "batches": []}

    # 窗口 2026-11-10：两个分区都到齐，生成第一份完整快照
    first_batches: dict[str, PartitionBatch] = {}
    for region in REGIONS:
        result = engine.ingest_batch(
            _batch_for(seg_high, all_records, date=DATES[0], region=region)
        )
        summary["batches"].append(result)
    for region in REGIONS:
        batch = _batch_for(seg_app, all_records, date=DATES[0], region=region)
        first_batches[f"{seg_app}:{region}"] = batch
        engine.ingest_batch(batch)

    # 窗口 2026-11-11：两分区到齐，生成第二版快照与覆盖对象差异
    for region in REGIONS:
        result = engine.ingest_batch(
            _batch_for(seg_high, all_records, date=DATES[1], region=region)
        )
        summary["batches"].append(result)

    # 边界冲突：11-11 的事件数据在 11-12 才到达（跨日），仍只归一个窗口
    conflict_batch = _batch_for(
        seg_app, all_records, date=DATES[1], region=REGIONS[0], arrival_day=DATES[2]
    )
    engine.ingest_batch(conflict_batch)
    engine.ingest_batch(
        _batch_for(seg_app, all_records, date=DATES[1], region=REGIONS[1])
    )

    # 窗口 2026-11-12：只到 east，west 缺失，窗口不完整、不出快照
    partial = engine.ingest_batch(
        _batch_for(seg_high, all_records, date=DATES[2], region=REGIONS[0])
    )
    summary["partial_window"] = partial

    # 演示失败恢复：再构造一个完整窗口后人为打失败一个分区
    engine.ingest_batch(
        _batch_for(seg_high, all_records, date=DATES[2], region=REGIONS[1])
    )
    failed_state = engine.mark_partition_failed(seg_high, DATES[2], REGIONS[1])
    summary["failed_partition"] = failed_state
    # 用新批次从缺失分区恢复（同分区新 batch_id，走重算路径）
    recovery_batch = _batch_for(seg_high, all_records, date=DATES[2], region=REGIONS[1])
    recovery = engine.recover_window(seg_high, DATES[2], [recovery_batch])
    summary["recovery"] = recovery

    # 重复提交幂等演示：同一 batch_id 原样再发一次，不产生第二份快照
    duplicate = engine.ingest_batch(first_batches[f"{seg_app}:{REGIONS[0]}"])
    summary["duplicate_submission"] = duplicate
    summary["seeded_at"] = utc_now()
    return summary


_engine: SegmentProfileEngine | None = None
_consumer: SnapshotConsumer | None = None
_population: PopulationRepository | None = None


def get_engine() -> SegmentProfileEngine:
    global _engine, _consumer, _population
    if _engine is None:
        _engine, _consumer, _population = build_engine()
        seed_demo_data(_engine, _population)
    return _engine


def get_consumer() -> SnapshotConsumer:
    get_engine()
    assert _consumer is not None
    return _consumer


def get_population() -> PopulationRepository:
    get_engine()
    assert _population is not None
    return _population
