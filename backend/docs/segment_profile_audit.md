# 分群画像增量快照引擎 —— 可审计逻辑说明

## 1. 背景与要解决的问题

旧口径的两个痛点：

1. 分群画像每天全量重算，覆盖对象数稍有变化就把**所有**画像差异标成过期；
2. 发布后的历史快照被最新对比基准**就地覆盖**，覆盖对象数与画像差异无法追溯。

本引擎把口径改为「分区增量合并 + 快照只追加 + 差异按范围失效」，代码位于
`backend/app/segment/`，全部使用标准库，无外部依赖。

## 2. 模块职责（每个判断只有一处实现）

| 模块 | 职责 | 关键约束 |
| --- | --- | --- |
| `models.py` | 数据模型：快照、差异、批次、窗口态、冲突、审计 | 快照为 frozen dataclass，不可变 |
| `rule_eval.py` | 筛选规则结构化表达式求值 | 纯函数，过滤与重放口径一致 |
| `repository.py` | 目标人群只读仓库、规则版本注册表、快照只追加仓库 | 规则只注册新版本不改历史；快照无 update/delete |
| `merge.py` | **全项目唯一的增量合并实现** | 分区重算回滚后复用同一 `add_partition` |
| `invalidation.py` | 画像差异的分区级、窗口级失效 | 不相关分区/其它窗口的差异保持 active |
| `boundary.py` | 事件时间/到达时间边界冲突裁决 | 一个批次只归一个窗口 |
| `engine.py` | 分群锁编排：幂等接入、失败恢复、快照唯一生效 | 每分群一把 RLock |
| `consumer.py` | 事件分析下游只读消费 | 跟随 eventanalysis 已启用模型定位下游步骤 |
| `bootstrap.py` | 装配与演示数据 | 只走公开 API 引导 |

## 3. 增量合并为什么只有一份实现（审计重点）

`MergeAccumulator` 是窗口级累加器，合并入口只有两个：

- `add_partition(batch, records, rule)`：分区首次合并；
- `replace_partition(...)`：分区重算/恢复，先摘除该分区的旧贡献
  （成员、维度聚合、批次登记各回滚一次，并追加 `partition_rollback` 留痕），
  然后**直接调用 `add_partition`**，不重写任何合并逻辑。

代码里不存在按分区名（east/west/…）分支的合并代码。任何分区、任何策略、
首次到达还是失败恢复，最终都经过同一段成员去重、规则过滤、维度聚合代码。

每次合并追加一条 `MergeTraceEntry`，记录：批次号、分区、流入对象数、
未知 ID、命中数、新增对象、重复对象、时间戳。相同输入可按留痕完整重放。

## 4. 快照与差异的追溯链

### 4.1 快照只追加，不覆盖

- 快照带 `version`、`fingerprint`（成员集合 + 画像 + 分区批次序列 + 规则版本
  + 边界冲突 ID 的 SHA-256）；
- 新版本生效时旧版本只改状态为 `superseded`，记录永久保留；
- 同指纹重复固化走 `snapshot_dedup_skipped`，**绝不产生第二份快照**；
- 一个窗口在任意时刻只有一个 `effective` 快照指针。

### 4.2 覆盖对象数与画像差异的连续链

- 新窗口首份快照：与分群时间链上的上一个有效快照对比（跨日基准不丢失）；
- 同窗口分区重算：与同窗口上一版对比，旧差异按范围失效后生成新差异；
- 每个差异保存 `count_before/count_after`、新增/移除对象 ID、
  画像维度增量、`affected_partitions`、状态与失效原因；
- 首版差异 `count_before=0`、added 为全量，从第一版起即可追溯。

### 4.3 差异失效范围（替代「全量过期」）

差异只在同时满足以下条件时才允许标 stale：

1. 与当前重算属于**同一窗口**（窗口隔离，A 窗口重算不动 B 窗口差异）；
2. 差异的 `affected_partitions` 与本次变化分区**有交集**。

失效只改状态并写 `stale_reason`，不删除记录；失效动作本身产生
`diff_invalidate_scoped` 审计事件。

## 5. 事件时间 / 到达时间边界冲突的代价权衡

| 策略 | 裁决口径 | 业务代价 |
| --- | --- | --- |
| `EVENT_TIME`（默认） | 按事件日归窗，迟到数据修正历史窗口并追加快照版本 | 语义最准；下游需容忍历史窗口版本追加 |
| `ARRIVAL_TIME` | 按到达日归窗，窗口定稿后不变 | 实现最简单、下游最稳；跨日事件有固定口径偏差 |
| `ARRIVAL_FIRST_LOCK` | 同窗口内分区首达窗口锁定，补数必须回同一窗口 | 兼顾稳定与首达语义；需维护窗口内接收态，异常补数要求人工裁决 |

不变量：三种策略下，一个批次都只返回一个 `window_id`，覆盖对象数因此只可能
进入一个窗口的一个完整快照。每次冲突生成 `BoundaryConflict` 留痕，含两个
时间戳、策略、裁决窗口与理由。

## 6. 并发、幂等与失败恢复

### 6.1 并发只出一份完整快照

- 每个分群一把可重入锁，批次接收→合并→快照生效在锁内串行；
- 批次级幂等：同 `batch_id` 重复提交直接返回首次结果
  （审计 `batch_duplicate_ignored`）；
- 指纹级幂等：同输入同指纹，存储层拒绝第二份等效快照；
- 并发测试见 `tests/test_segment_engine.py::ConcurrencyTests`。

### 6.2 失败后从缺失分区恢复

- 窗口按规则的 `expected_partitions` 判定完整性，缺失/失败分区可随时补提；
- `mark_partition_failed` 摘除生效指针并累加 attempts；
- `recover_window` 复用与首次到达完全相同的合并路径，补齐后自动固化快照；
- 未齐窗口不会产生任何快照（半成品不落库）。

## 7. 共享数据的职责边界

- 目标人群数据：`PopulationRepository` 只暴露读接口（get/get_many/count），
  引擎与下游消费器均无法改写；
- 分群筛选规则：`RuleRegistry.register` 对同口径提交幂等，重复提交返回当前
  版本，不产生第二份规则版本；
- 事件分析消费器（`consumer.py`）无任何写引擎方法，只记录消费了哪个
  快照版本、覆盖对象数与指纹，下游步骤从 eventanalysis 模块状态为
  「已启用」的模型解析，跟随现有调用定位。

## 8. 审计事件清单

`rule_register`、`batch_ingested`、`batch_duplicate_ignored`、
`batch_merge_rejected`、`partition_marked_failed`、
`window_recover_started`、`snapshot_published`、`snapshot_dedup_skipped`、
`diff_invalidate_scoped`、`snapshot_consumed`。

查询入口：`GET /api/segmentprofile/engine/audit`，可按分群、窗口、动作过滤。

## 9. HTTP 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/segmentprofile/engine/overview` | 引擎总览 |
| POST | `/api/segmentprofile/engine/segments` | 注册分群规则（幂等） |
| GET | `/api/segmentprofile/engine/segments/{ref}/windows` | 窗口与分区接收态、合并留痕 |
| POST | `/api/segmentprofile/engine/segments/{ref}/batches` | 接入分区批次 |
| POST | `/api/segmentprofile/engine/segments/{ref}/windows/{w}/fail` | 标记分区失败 |
| POST | `/api/segmentprofile/engine/segments/{ref}/windows/{w}/recover` | 缺失分区恢复 |
| GET | `/api/segmentprofile/engine/segments/{ref}/snapshots` | 快照历史（含 superseded） |
| GET | `/api/segmentprofile/engine/segments/{ref}/diffs` | 覆盖对象与画像差异 |
| GET | `/api/segmentprofile/engine/segments/{ref}/conflicts` | 边界冲突裁决留痕 |
| GET | `/api/segmentprofile/engine/audit` | 审计轨迹 |
| GET | `/api/eventanalysis/snapshot-consumption/downstream-steps` | 下游步骤定位 |
| POST | `/api/eventanalysis/snapshot-consumption/sync` | 事件分析同步消费 |
| GET | `/api/eventanalysis/snapshot-consumption/{ref}/history` | 消费历史 |
