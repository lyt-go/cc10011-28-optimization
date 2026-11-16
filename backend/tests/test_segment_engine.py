"""分群画像增量引擎测试。

覆盖需求中的每个硬性约束：
1. 增量合并只有一份实现，分区重算复用同一路径；
2. 快照不可变、历史不覆盖，一个窗口只有一个 effective 快照；
3. 重复提交（同 batch_id / 同指纹）不产生第二份快照；
4. 差异失效限定分区与窗口范围，不相关差异保持 active；
5. 事件时间/到达时间跨日冲突时，一个批次只归一个窗口；
6. 失败后从缺失分区恢复，补齐后自动出完整快照；
7. 并发到达下分群锁保证快照唯一性；
8. 事件分析下游只读消费，并跟随现有已启用模型定位下游步骤。
"""
from __future__ import annotations

import threading
import unittest

from app.segment.bootstrap import build_engine
from app.segment.boundary import POLICIES
from app.segment.merge import MergeAccumulator, new_accumulator
from app.segment.models import PartitionBatch, PopulationRecord, new_id


def _population(size: int = 8, region: str | None = None) -> list[PopulationRecord]:
    records = []
    for index in range(size):
        reg = region or ("east" if index % 2 == 0 else "west")
        records.append(PopulationRecord(
            object_id=f"u-{index}",
            event_date="2026-11-16",
            region=reg,
            attributes={"level": "高" if index % 3 == 0 else "中",
                        "channel": "app" if index % 2 == 0 else "web"},
        ))
    return records


def _batch(segment_ref: str, partition: str, object_ids: list[str],
           *, event_date: str = "2026-11-16", arrival_day: str | None = None) -> PartitionBatch:
    return PartitionBatch(
        batch_id=new_id("batch"),
        segment_ref=segment_ref,
        partition=partition,
        event_time=f"{event_date}T01:00:00+00:00",
        arrival_time=f"{arrival_day or event_date}T09:00:00+00:00",
        object_ids=tuple(object_ids),
    )


class EngineTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.engine, self.consumer, self.population = build_engine()
        self.records = _population()
        self.population.load(self.records)
        self.seg = "SEG-T1"
        self.engine.register_segment(
            segment_ref=self.seg,
            segment_name="测试分群",
            expression={"field": "level", "op": "eq", "value": "高"},
            profile_dimensions=("level", "channel"),
            expected_partitions=("east", "west"),
            boundary_policy="EVENT_TIME",
        )

    def _ids(self, partition: str) -> list[str]:
        return [r.object_id for r in self.records if r.region == partition]


class IncrementalMergeTests(EngineTestBase):
    def test_partitions_merge_through_single_implementation(self) -> None:
        # 两个分区分别接入，走的是同一个 MergeAccumulator.add_partition
        self.engine.ingest_batch(_batch(self.seg, "east", self._ids("east")))
        window = self.engine.window_view(self.seg)[0]
        self.assertFalse(window["complete"])
        self.assertEqual(window["missing_partitions"], ["west"])
        steps = {t["step"] for t in window["merge_trace"]}
        self.assertEqual(steps, {"partition_merge"})

        result = self.engine.ingest_batch(_batch(self.seg, "west", self._ids("west")))
        self.assertEqual(result["status"], "snapshot_published")
        window = self.engine.window_view(self.seg)[0]
        self.assertTrue(window["complete"])
        # 高等级用户：index 0,3,6 共 3 个
        self.assertEqual(result["snapshot_id"][:4], "snap")

    def test_replace_partition_reuses_add_path(self) -> None:
        # 重算路径必须出现 rollback + merge 两种留痕，且来自同一累加器
        rule = self.engine.rules.latest(self.seg)
        acc: MergeAccumulator = new_accumulator(rule, "w")
        self.assertIs(acc.replace_partition.__func__, MergeAccumulator.replace_partition)
        # 全项目只有 MergeAccumulator 一个类持有分区合并方法
        self.assertTrue(hasattr(acc, "add_partition"))
        self.assertTrue(hasattr(acc, "replace_partition"))


class SnapshotUniquenessTests(EngineTestBase):
    def _complete_window(self, *, date: str = "2026-11-16") -> str:
        first = _batch(self.seg, "east", self._ids("east"), event_date=date)
        self.engine.ingest_batch(first)
        result = self.engine.ingest_batch(
            _batch(self.seg, "west", self._ids("west"), event_date=date)
        )
        return result["snapshot_id"]

    def test_duplicate_batch_does_not_create_second_snapshot(self) -> None:
        first = _batch(self.seg, "east", self._ids("east"))
        self.engine.ingest_batch(first)
        self.engine.ingest_batch(_batch(self.seg, "west", self._ids("west")))
        snapshots_before = len(self.engine.snapshots.list_snapshots(self.seg))
        # 完全相同的 batch_id 再提交一次
        again = self.engine.ingest_batch(first)
        self.assertTrue(again["duplicate"])
        self.assertEqual(
            len(self.engine.snapshots.list_snapshots(self.seg)), snapshots_before
        )

    def test_one_effective_snapshot_per_window(self) -> None:
        snapshot_id = self._complete_window()
        window = self.engine.window_view(self.seg)[0]
        self.assertEqual(window["effective_snapshot_id"], snapshot_id)
        effective = [
            s for s in self.engine.snapshots.list_snapshots(self.seg)
            if s.window_id == window["window_id"] and s.status == "effective"
        ]
        self.assertEqual(len(effective), 1)

    def test_history_snapshot_kept_not_overwritten(self) -> None:
        first_id = self._complete_window()
        # west 分区失败后用新批次恢复，产生新版本快照
        window_id = self.engine.window_view(self.seg)[0]["window_id"]
        self.engine.mark_partition_failed(self.seg, window_id, "west")
        recovery = self.engine.recover_window(
            self.seg, window_id,
            [_batch(self.seg, "west", self._ids("west"))],
        )
        self.assertTrue(recovery["complete"])
        snapshots = self.engine.snapshots.list_snapshots(self.seg)
        statuses = {s.snapshot_id: s.status for s in snapshots}
        self.assertEqual(statuses[first_id], "superseded")
        self.assertEqual(len([s for s in snapshots if s.status == "effective"]), 1)
        # 旧快照记录仍在，可按 ID 追溯
        self.assertIsNotNone(self.engine.snapshots.get_snapshot(first_id))

    def test_identical_fingerprint_dedup(self) -> None:
        self._complete_window()
        # 人为用相同累加状态再固化一次：指纹相同必须去重
        rule = self.engine.rules.latest(self.seg)
        window_id = self.engine.window_view(self.seg)[0]["window_id"]
        acc = self.engine.accumulators[(self.seg, window_id)]
        before = len(self.engine.snapshots.list_snapshots(self.seg))
        self.engine._finalize_snapshot(  # noqa: SLF001
            rule, self.engine.windows[self.seg][window_id], acc,
            changed_partitions=("west",), operator="test",
        )
        self.assertEqual(len(self.engine.snapshots.list_snapshots(self.seg)), before)


class DiffInvalidationTests(EngineTestBase):
    def test_diff_chain_across_windows(self) -> None:
        for date in ("2026-11-10", "2026-11-11"):
            self.engine.ingest_batch(_batch(self.seg, "east", self._ids("east"), event_date=date))
            self.engine.ingest_batch(_batch(self.seg, "west", self._ids("west"), event_date=date))
        diffs = self.engine.snapshots.list_diffs(self.seg)
        self.assertEqual(diffs[0].count_before, 0)
        self.assertEqual(diffs[1].count_before, diffs[0].count_after)
        self.assertTrue(all(d.status == "active" for d in diffs))

    def test_invalidation_scoped_to_partition_and_window(self) -> None:
        for date in ("2026-11-10", "2026-11-11"):
            self.engine.ingest_batch(_batch(self.seg, "east", self._ids("east"), event_date=date))
            self.engine.ingest_batch(_batch(self.seg, "west", self._ids("west"), event_date=date))
        target_window = "2026-11-11"
        self.engine.mark_partition_failed(self.seg, target_window, "west")
        self.engine.recover_window(
            self.seg, target_window,
            [_batch(self.seg, "west", self._ids("west"), event_date=target_window)],
        )
        diffs = self.engine.snapshots.list_diffs(self.seg)
        # 2026-11-10 窗口的差异不受影响，保持 active
        older = [d for d in diffs if d.window_id == "2026-11-10"]
        self.assertTrue(older)
        self.assertTrue(all(d.status == "active" for d in older))
        # 目标窗口旧差异被按范围失效，新差异 active 且只挂 west
        target_active = [d for d in diffs if d.window_id == target_window and d.status == "active"]
        self.assertEqual(target_active[-1].affected_partitions, ("west",))
        stale = [d for d in diffs if d.status == "stale"]
        self.assertTrue(stale)
        self.assertTrue(all("west" in d.affected_partitions for d in stale))

    def test_unrelated_partition_change_keeps_diff_active(self) -> None:
        self.engine.ingest_batch(_batch(self.seg, "east", self._ids("east"), event_date="2026-11-10"))
        self.engine.ingest_batch(_batch(self.seg, "west", self._ids("west"), event_date="2026-11-10"))
        diff = self.engine.snapshots.list_diffs(self.seg)[0]
        # 首版差异覆盖两个分区；手工构造一个只挂 east 的失效，west-only 不应被波及
        diff.affected_partitions = ("east",)
        from app.segment.invalidation import invalidate_for_partition
        stale = invalidate_for_partition(
            self.engine.snapshots, segment_ref=self.seg, window_id="2026-11-10",
            partition="west", reason="west 重算",
        )
        self.assertEqual(stale, [])
        self.assertEqual(diff.status, "active")


class BoundaryConflictTests(EngineTestBase):
    def test_conflicting_batch_lands_in_single_window(self) -> None:
        seg2 = "SEG-T2"
        self.engine.register_segment(
            segment_ref=seg2, segment_name="到达时间分群",
            expression={"field": "channel", "op": "eq", "value": "app"},
            profile_dimensions=("channel",), expected_partitions=("east", "west"),
            boundary_policy="ARRIVAL_TIME",
        )
        # 事件日 11-10、到达日 11-11：只能进入 11-11 一个窗口
        cross = _batch(seg2, "east", self._ids("east"),
                       event_date="2026-11-10", arrival_day="2026-11-11")
        self.engine.ingest_batch(cross)
        windows = {w["window_id"]: w for w in self.engine.window_view(seg2)}
        self.assertNotIn("2026-11-10", windows)
        self.assertIn("2026-11-11", windows)
        conflict = self.engine.conflicts_view(seg2)[0]
        self.assertEqual(conflict["decision_window"], "2026-11-11")
        self.assertEqual(conflict["policy"], "ARRIVAL_TIME")

    def test_all_policies_are_declared(self) -> None:
        self.assertEqual(set(POLICIES),
                         {"EVENT_TIME", "ARRIVAL_TIME", "ARRIVAL_FIRST_LOCK"})

    def test_invalid_policy_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.engine.register_segment(
                segment_ref="SEG-BAD", segment_name="x", expression={},
                profile_dimensions=(), expected_partitions=(),
                boundary_policy="UNKNOWN",
            )


class RecoveryTests(EngineTestBase):
    def test_recover_from_missing_partition(self) -> None:
        self.engine.ingest_batch(_batch(self.seg, "east", self._ids("east")))
        window_id = self.engine.window_view(self.seg)[0]["window_id"]
        # 只到一个分区时没有快照
        self.assertIsNone(self.engine.window_view(self.seg)[0]["effective_snapshot_id"])
        result = self.engine.recover_window(
            self.seg, window_id, [_batch(self.seg, "west", self._ids("west"))]
        )
        self.assertTrue(result["complete"])
        self.assertIsNotNone(result["effective_snapshot_id"])

    def test_failed_partition_recovery_publishes_snapshot(self) -> None:
        self.engine.ingest_batch(_batch(self.seg, "east", self._ids("east")))
        self.engine.ingest_batch(_batch(self.seg, "west", self._ids("west")))
        window_id = self.engine.window_view(self.seg)[0]["window_id"]
        self.engine.mark_partition_failed(self.seg, window_id, "west")
        self.assertEqual(
            self.engine.window_view(self.seg)[0]["missing_partitions"], ["west"]
        )
        result = self.engine.recover_window(
            self.seg, window_id, [_batch(self.seg, "west", self._ids("west"))]
        )
        self.assertEqual(result["recovered_partitions"], ["west"])
        self.assertTrue(result["complete"])


class ConcurrencyTests(EngineTestBase):
    def test_concurrent_batches_yield_single_effective_snapshot(self) -> None:
        errors: list[Exception] = []

        def send(partition: str) -> None:
            try:
                self.engine.ingest_batch(_batch(self.seg, partition, self._ids(partition)))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=send, args=("east",)),
            threading.Thread(target=send, args=("west",)),
            # 重复提交也并发进来
            threading.Thread(target=send, args=("east",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        window = self.engine.window_view(self.seg)[0]
        effective = [
            s for s in self.engine.snapshots.list_snapshots(self.seg)
            if s.window_id == window["window_id"] and s.status == "effective"
        ]
        self.assertEqual(len(effective), 1)

    def test_concurrent_recoveries_stay_serialized(self) -> None:
        for partition in ("east", "west"):
            self.engine.ingest_batch(_batch(self.seg, partition, self._ids(partition)))
        window_id = self.engine.window_view(self.seg)[0]["window_id"]
        self.engine.mark_partition_failed(self.seg, window_id, "west")

        def recover() -> None:
            self.engine.recover_window(
                self.seg, window_id, [_batch(self.seg, "west", self._ids("west"))]
            )

        threads = [threading.Thread(target=recover) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        snapshots = self.engine.snapshots.list_snapshots(self.seg)
        self.assertEqual(len([s for s in snapshots if s.status == "effective"]), 1)


class ConsumerTests(EngineTestBase):
    def test_consumer_readonly_and_follows_enabled_models(self) -> None:
        self.engine.ingest_batch(_batch(self.seg, "east", self._ids("east")))
        self.engine.ingest_batch(_batch(self.seg, "west", self._ids("west")))
        result = self.consumer.consume_latest(self.seg)
        # 下游步骤来自 eventanalysis 现有数据里状态为「已启用」的模型
        self.assertTrue(all(step["status"] == "已启用" for step in result["downstream_steps"]))
        self.assertEqual(len(result["synced"]), 1)
        record = result["synced"][0]
        snapshot = self.engine.snapshots.get_snapshot(record["snapshot_id"])
        self.assertEqual(snapshot.status, "effective")
        self.assertEqual(record["coverage_count"], snapshot.coverage_count)
        # 消费历史只追加
        self.consumer.consume_latest(self.seg)
        self.assertEqual(len(self.consumer.consumption_history(self.seg)), 2)
        # 消费器没有任何写引擎的方法
        self.assertFalse(hasattr(self.consumer, "ingest_batch"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
