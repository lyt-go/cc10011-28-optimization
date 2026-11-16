import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENGINE = '/api/segmentprofile/engine'
const CONSUME = '/api/eventanalysis/snapshot-consumption'

type Overview = {
  population_size: number
  audit_event_count: number
  boundary_policies: string[]
  segments: Array<{
    segment_ref: string
    segment_name: string
    rule_version: number
    boundary_policy: string
    expected_partitions: string[]
    window_count: number
    complete_windows: number
    snapshot_count: number
    diff_count: number
  }>
}

type SnapshotRow = {
  snapshot_id: string
  window_id: string
  version: number
  status: string
  coverage_count: number
  profile: Record<string, Record<string, number>>
  partition_ids: string[]
  fingerprint: string
  created_at: string
}

type DiffRow = {
  diff_id: string
  window_id: string
  status: string
  stale_reason: string | null
  count_before: number
  count_after: number
  added: string[]
  removed: string[]
  profile_delta: Record<string, Record<string, number>>
  affected_partitions: string[]
}

type ConflictRow = {
  conflict_id: string
  window_id: string
  partition: string
  policy: string
  decision_window: string
  event_time: string
  arrival_time: string
  reason: string
}

type WindowRow = {
  window_id: string
  complete: boolean
  missing_partitions: string[]
  effective_snapshot_id: string | null
  partitions: Array<{ partition: string; status: string; attempts: number; object_count: number }>
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) {
    throw new Error(`接口返回 ${response.status}`)
  }
  return response.json() as Promise<T>
}

function StatusTag({ status }: { status: string }) {
  const cls = status === 'effective' || status === 'active' ? 'tag ok' : 'tag muted'
  return <span className={cls}>{status}</span>
}

export default function SegmentEnginePanel() {
  const [overview, setOverview] = useState<Overview | null>(null)
  const [selected, setSelected] = useState('')
  const [snapshots, setSnapshots] = useState<SnapshotRow[]>([])
  const [diffs, setDiffs] = useState<DiffRow[]>([])
  const [conflicts, setConflicts] = useState<ConflictRow[]>([])
  const [windows, setWindows] = useState<WindowRow[]>([])
  const [consumeMsg, setConsumeMsg] = useState('')
  const [error, setError] = useState('')

  const loadOverview = useCallback(async () => {
    const data = await getJson<Overview>(`${ENGINE}/overview`)
    setOverview(data)
    setSelected((prev) => prev || (data.segments[0]?.segment_ref ?? ''))
  }, [])

  const loadDetail = useCallback(async (segmentRef: string) => {
    if (!segmentRef) return
    const [snap, diff, conf, win] = await Promise.all([
      getJson<{ snapshots: SnapshotRow[] }>(`${ENGINE}/segments/${segmentRef}/snapshots`),
      getJson<{ diffs: DiffRow[] }>(`${ENGINE}/segments/${segmentRef}/diffs`),
      getJson<{ conflicts: ConflictRow[] }>(`${ENGINE}/segments/${segmentRef}/conflicts`),
      getJson<{ windows: WindowRow[] }>(`${ENGINE}/segments/${segmentRef}/windows`),
    ])
    setSnapshots(snap.snapshots)
    setDiffs(diff.diffs)
    setConflicts(conf.conflicts)
    setWindows(win.windows)
  }, [])

  useEffect(() => {
    void loadOverview().catch((e: unknown) => setError(e instanceof Error ? e.message : '引擎总览读取失败'))
  }, [loadOverview])

  useEffect(() => {
    if (selected) {
      void loadDetail(selected).catch((e: unknown) => setError(e instanceof Error ? e.message : '引擎明细读取失败'))
    }
  }, [selected, loadDetail])

  const segmentMeta = useMemo(
    () => overview?.segments.find((item) => item.segment_ref === selected),
    [overview, selected],
  )

  async function syncConsumption() {
    setConsumeMsg('')
    try {
      const response = await request(`${CONSUME}/sync`, {
        method: 'POST',
        body: JSON.stringify({ segment_ref: selected }),
      })
      if (!response.ok) throw new Error('事件分析同步消费失败')
      const data = await response.json()
      setConsumeMsg(`事件分析已同步 ${data.synced.length} 个窗口的 effective 快照，下游步骤 ${data.downstream_steps.length} 个`)
    } catch (e) {
      setConsumeMsg(e instanceof Error ? e.message : '同步失败')
    }
  }

  return (
    <section className="engine-panel">
      <div className="engine-head">
        <div>
          <h3>分群画像增量引擎</h3>
          <p className="page-desc">
            增量合并单一实现 · 快照只追加不覆盖 · 差异按分区/窗口范围失效 · 分群锁保证并发只出一份完整快照
          </p>
        </div>
        <div className="engine-actions">
          {overview && (
            <span className="tag info">
              目标人群 {overview.population_size} · 审计事件 {overview.audit_event_count}
            </span>
          )}
          <button className="btn primary" type="button" onClick={() => void syncConsumption()}>
            事件分析同步消费
          </button>
        </div>
      </div>
      {consumeMsg && <p className="info-text">{consumeMsg}</p>}
      {error && <p className="error-text">{error}</p>}

      <div className="filter-bar">
        <label className="filter-item">
          <span>分群</span>
          <select value={selected} onChange={(e) => setSelected(e.target.value)}>
            {overview?.segments.map((item) => (
              <option key={item.segment_ref} value={item.segment_ref}>
                {item.segment_ref}（{item.segment_name}）
              </option>
            ))}
          </select>
        </label>
        {segmentMeta && (
          <span className="tag info">
            规则 v{segmentMeta.rule_version} · {segmentMeta.boundary_policy} · 预期分区
            {segmentMeta.expected_partitions.join('/')}
          </span>
        )}
      </div>

      <h4>窗口与分区接收</h4>
      <table className="data-table">
        <thead>
          <tr>
            <th>窗口</th><th>分区状态</th><th>缺失分区</th><th>是否完整</th><th>生效快照</th>
          </tr>
        </thead>
        <tbody>
          {windows.map((w) => (
            <tr key={w.window_id}>
              <td>{w.window_id}</td>
              <td>
                {w.partitions.map((p) => (
                  <span key={p.partition} className={`tag ${p.status === 'received' ? 'ok' : 'danger'}`}>
                    {p.partition}:{p.status}/第{p.attempts}次/{p.object_count}人
                  </span>
                ))}
              </td>
              <td>{w.missing_partitions.length ? w.missing_partitions.join('、') : '—'}</td>
              <td>{w.complete ? '完整' : '未齐'}</td>
              <td className="mono">{w.effective_snapshot_id ?? '—'}</td>
            </tr>
          ))}
          {windows.length === 0 && (
            <tr><td className="empty-state" colSpan={5}>暂无窗口数据</td></tr>
          )}
        </tbody>
      </table>

      <h4>画像快照（历史保留，不覆盖）</h4>
      <table className="data-table">
        <thead>
          <tr>
            <th>版本</th><th>窗口</th><th>状态</th><th>覆盖对象数</th><th>画像维度分布</th><th>分区</th><th>指纹</th>
          </tr>
        </thead>
        <tbody>
          {snapshots.map((s) => (
            <tr key={s.snapshot_id}>
              <td>v{s.version}</td>
              <td>{s.window_id}</td>
              <td><StatusTag status={s.status} /></td>
              <td><strong>{s.coverage_count}</strong></td>
              <td className="mono small">
                {Object.entries(s.profile).map(([dim, dist]) => (
                  <div key={dim}>{dim}: {JSON.stringify(dist)}</div>
                ))}
              </td>
              <td>{s.partition_ids.join('、')}</td>
              <td className="mono small">{s.fingerprint.slice(0, 12)}…</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h4>覆盖对象与画像差异（按分区范围失效）</h4>
      <table className="data-table">
        <thead>
          <tr>
            <th>窗口</th><th>状态</th><th>对象数变化</th><th>新增</th><th>移除</th><th>画像增量</th><th>失效范围分区</th><th>失效原因</th>
          </tr>
        </thead>
        <tbody>
          {diffs.map((d) => (
            <tr key={d.diff_id}>
              <td>{d.window_id}</td>
              <td><StatusTag status={d.status} /></td>
              <td>{d.count_before} → {d.count_after}</td>
              <td className="small">{d.added.join('、') || '—'}</td>
              <td className="small">{d.removed.join('、') || '—'}</td>
              <td className="mono small">{JSON.stringify(d.profile_delta)}</td>
              <td>{d.affected_partitions.join('、')}</td>
              <td className="small">{d.stale_reason ?? '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h4>事件时间 / 到达时间边界冲突裁决</h4>
      <table className="data-table">
        <thead>
          <tr>
            <th>窗口</th><th>分区</th><th>策略</th><th>裁决窗口</th><th>事件时间</th><th>到达时间</th><th>裁决理由</th>
          </tr>
        </thead>
        <tbody>
          {conflicts.map((c) => (
            <tr key={c.conflict_id}>
              <td>{c.window_id}</td>
              <td>{c.partition}</td>
              <td><span className="tag info">{c.policy}</span></td>
              <td><strong>{c.decision_window}</strong></td>
              <td className="small">{c.event_time.slice(0, 16)}</td>
              <td className="small">{c.arrival_time.slice(0, 16)}</td>
              <td className="small">{c.reason}</td>
            </tr>
          ))}
          {conflicts.length === 0 && (
            <tr><td className="empty-state" colSpan={7}>该分群暂无跨日边界冲突</td></tr>
          )}
        </tbody>
      </table>
    </section>
  )
}
