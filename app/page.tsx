'use client';

import { useEffect, useMemo, useState } from 'react';

type CohortHealth = { request_count: number; error_count: number; error_rate: number; avg_latency_ms: number };
type HealthPoint = { window_end: string; stable: CohortHealth; canary: CohortHealth; source: 'local' | 'flink' };
type TimelineEvent = { event_id: string; event_type: string; title: string; detail: string; tone: 'neutral' | 'violet' | 'amber' | 'red' | 'green'; occurred_at: string };
type DemoSnapshot = {
  release_id: string; service: string;
  phase: 'READY' | 'CANARY_RUNNING' | 'REGRESSION' | 'ROLLBACK_PENDING' | 'ROLLED_BACK';
  stable_version: string; canary_version: string; stable_percent: number; canary_percent: number;
  started_at: string | null;
  latest_decision: null | { decision_id: string; reason: string; reason_code: string; canary_error_rate: number; canary_avg_latency_ms: number };
  latest_action: null | { decision_id: string; detail: string };
  health_history: HealthPoint[]; timeline: TimelineEvent[];
  metrics: { events_per_second: number; total_requests: number; canary_requests: number; canary_errors: number; time_to_detect_seconds: number | null; recovery_time_seconds: number | null; errors_avoided_estimate: number; sessions_protected_estimate: number; estimate_baseline_seconds: number };
  pipeline: { mode: 'confluent' | 'local_preview'; health_source: 'flink' | 'deterministic_local_twin'; connector_delivery: boolean };
};

const emptySnapshot: DemoSnapshot = {
  release_id: 'release-initializing', service: 'checkout-api', phase: 'READY', stable_version: 'v2.3.4', canary_version: 'v2.4.0', stable_percent: 100, canary_percent: 0, started_at: null,
  latest_decision: null, latest_action: null, health_history: [], timeline: [],
  metrics: { events_per_second: 50, total_requests: 0, canary_requests: 0, canary_errors: 0, time_to_detect_seconds: null, recovery_time_seconds: null, errors_avoided_estimate: 0, sessions_protected_estimate: 0, estimate_baseline_seconds: 300 },
  pipeline: { mode: 'local_preview', health_source: 'deterministic_local_twin', connector_delivery: false },
};

const phaseCopy = {
  READY: { eyebrow: 'PRODUCTION HEALTHY', title: 'Production is protected', body: 'Stable v2.3.4 is serving 100% of traffic. The safety net is ready for a canary.', tone: 'green' },
  CANARY_RUNNING: { eyebrow: 'CANARY OBSERVATION', title: 'Canary is healthy', body: 'v2.4.0 is receiving 10% of production traffic while Flink compares both cohorts.', tone: 'violet' },
  REGRESSION: { eyebrow: 'REGRESSION STREAMING', title: 'Customer health is degrading', body: 'The canary is producing elevated latency and errors. Flink is evaluating the live window.', tone: 'amber' },
  ROLLBACK_PENDING: { eyebrow: 'GUARDRAIL BREACHED', title: 'Rollback decision emitted', body: 'Flink wrote a durable ROLLBACK decision. HTTP Sink V2 is delivering the control action.', tone: 'red' },
  ROLLED_BACK: { eyebrow: 'CUSTOMERS PROTECTED', title: 'Rollback completed', body: 'v2.4.0 was isolated automatically. Stable traffic continued while the dashboard captured the evidence.', tone: 'green' },
} as const;

function fmtSeconds(value: number | null) { return value === null ? '—' : `${value.toFixed(1)}s`; }
function fmtTime(value: string | null) {
  if (!value) return '—';
  return new Intl.DateTimeFormat('en', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(new Date(value));
}

function MiniBars({ points, metric }: { points: HealthPoint[]; metric: 'latency' | 'errors' }) {
  const values = points.slice(-18);
  const ceiling = metric === 'latency' ? 750 : 25;
  return <div className="chart-shell" aria-label={`${metric} history for stable and canary releases`}>
    <div className="chart-grid" />
    {metric === 'errors' && <div className="slo-line"><span>5% guardrail</span></div>}
    <div className="chart-bars">
      {values.length === 0 && <p className="chart-empty">Waiting for the first streaming window…</p>}
      {values.map((point, index) => {
        const stableValue = metric === 'latency' ? point.stable.avg_latency_ms : point.stable.error_rate * 100;
        const canaryValue = metric === 'latency' ? point.canary.avg_latency_ms : point.canary.error_rate * 100;
        return <div className="bar-slot" key={`${point.window_end}-${index}`} title={`${fmtTime(point.window_end)} · stable ${stableValue.toFixed(1)} · canary ${canaryValue.toFixed(1)}`}>
          <i className="bar stable-bar" style={{ height: `${Math.max(stableValue > 0 ? 3 : 0, Math.min(100, stableValue / ceiling * 100))}%` }} />
          <i className={`bar canary-bar ${canaryValue > (metric === 'latency' ? 400 : 5) ? 'breach' : ''}`} style={{ height: `${Math.max(canaryValue > 0 ? 3 : 0, Math.min(100, canaryValue / ceiling * 100))}%` }} />
        </div>;
      })}
    </div>
  </div>;
}

function Metric({ label, value, detail, accent }: { label: string; value: string; detail: string; accent?: boolean }) {
  return <div className={`metric ${accent ? 'metric-accent' : ''}`}><p>{label}</p><strong>{value}</strong><span>{detail}</span></div>;
}

export default function Home() {
  const [snapshot, setSnapshot] = useState<DemoSnapshot>(emptySnapshot);
  const [connected, setConnected] = useState(false);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');
  useEffect(() => {
    const source = new EventSource('/api/events');
    source.onopen = () => setConnected(true);
    source.addEventListener('snapshot', (event) => { setSnapshot(JSON.parse((event as MessageEvent).data)); setConnected(true); });
    source.onerror = () => setConnected(false);
    return () => source.close();
  }, []);
  const act = async (path: string, body?: object) => {
    setBusy(true); setMessage('');
    try {
      const response = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
      const payload = await response.json() as DemoSnapshot & { detail?: string };
      if (!response.ok) throw new Error(payload.detail || 'The action could not be completed');
      setSnapshot(payload);
    } catch (error) { setMessage(error instanceof Error ? error.message : 'The action could not be completed'); }
    finally { setBusy(false); }
  };
  const latest = snapshot.health_history.at(-1);
  const canLaunch = snapshot.phase === 'READY' || snapshot.phase === 'ROLLED_BACK';
  const canInject = snapshot.phase === 'CANARY_RUNNING';
  const status = connected ? phaseCopy[snapshot.phase] : { eyebrow: 'RECONNECTING', title: 'Decision stream interrupted', body: 'ReleaseGuard is reconnecting to the live event feed.', tone: 'amber' as const };
  const postRollback = snapshot.phase === 'ROLLED_BACK';
  const canaryPeakLatency = useMemo(() => Math.max(0, ...snapshot.health_history.map((p) => p.canary.avg_latency_ms)), [snapshot.health_history]);
  const canaryPeakErrors = useMemo(() => Math.max(0, ...snapshot.health_history.map((p) => p.canary.error_rate * 100)), [snapshot.health_history]);

  return <main>
    <div className="ambient ambient-one" /><div className="ambient ambient-two" />
    <div className="dashboard">
      <header className="topbar">
        <div className="brand-group"><div className="brand-mark"><span>R</span><i /></div><div><h1>ReleaseGuard</h1><p>Streaming canary protection</p></div></div>
        <div className="header-stats"><span className="rate"><b>{snapshot.metrics.events_per_second * 60}</b> events/min</span><span className={`live-pill ${connected ? '' : 'offline'}`}><i />Confluent Cloud · {connected ? 'LIVE' : 'RECONNECTING'}</span></div>
      </header>

      <section className={`status-banner tone-${status.tone}`}>
        <div className="status-icon"><span>{snapshot.phase === 'ROLLED_BACK' ? '✓' : snapshot.phase === 'ROLLBACK_PENDING' ? '!' : '◆'}</span></div>
        <div className="status-copy"><p>{status.eyebrow}</p><h2>{status.title}</h2><span>{status.body}</span></div>
        <div className="status-proof">{postRollback ? <><strong>{snapshot.metrics.sessions_protected_estimate.toLocaleString()}</strong><span>projected sessions protected</span></> : <><strong>{snapshot.stable_percent}/{snapshot.canary_percent}</strong><span>stable / canary traffic</span></>}</div>
      </section>

      <section className="metric-strip">
        <Metric label="TRAFFIC" value={`${snapshot.stable_percent} / ${snapshot.canary_percent}`} detail="Stable / canary" />
        <Metric label="DETECT" value={fmtSeconds(snapshot.metrics.time_to_detect_seconds)} detail="Regression to decision" accent={postRollback} />
        <Metric label="RECOVER" value={fmtSeconds(snapshot.metrics.recovery_time_seconds)} detail="Decision to isolation" />
        <Metric label="ERRORS AVOIDED" value={snapshot.metrics.errors_avoided_estimate.toLocaleString()} detail="Demo estimate · 5 min baseline" />
        <Metric label="PROTECTED" value={snapshot.metrics.sessions_protected_estimate.toLocaleString()} detail="Projected sessions · demo" accent={postRollback} />
      </section>

      <div className="main-grid">
        <section className="panel release-card">
          <div className="panel-head"><div><p className="eyebrow">PRODUCTION · {snapshot.service.toUpperCase()}</p><h3>Release {snapshot.canary_version}</h3></div><span className={`phase-pill phase-${snapshot.phase.toLowerCase()}`}>{snapshot.phase.replaceAll('_', ' ')}</span></div>
          <div className="release-lanes">
            <div className="release-lane stable-lane"><span>STABLE</span><strong>{snapshot.stable_version}</strong><b>{snapshot.stable_percent}% traffic</b></div>
            <div className="lane-arrow"><i /><span>versus</span><i /></div>
            <div className="release-lane canary-lane"><span>CANARY</span><strong>{snapshot.canary_version}</strong><b>{snapshot.canary_percent}% traffic</b></div>
          </div>
          <div className="traffic-caption"><span>Live traffic allocation</span><b>{snapshot.stable_percent}% stable</b></div><div className="traffic-bar"><i style={{ width: `${snapshot.stable_percent}%` }} /><b style={{ width: `${snapshot.canary_percent}%` }} /></div>
          <div className="release-footer"><span>{snapshot.release_id}</span><span>{snapshot.started_at ? `Started ${fmtTime(snapshot.started_at)}` : 'Ready to launch'}</span></div>
        </section>

        <section className="panel controls-card">
          <div><p className="eyebrow">DEMO SCENARIO</p><h3>Controlled checkout regression</h3><span className="subcopy">Launch safely, inject a fault, and watch the streaming guardrail act.</span></div>
          <div className="controls">
            <button className="primary" disabled={!connected || busy || !canLaunch} onClick={() => act('/api/demo/canary', { version: 'v2.4.0', traffic_percent: 10 })}>{postRollback ? 'Replay canary' : 'Launch canary'}<span>→</span></button>
            <button className="danger" disabled={!connected || busy || !canInject} onClick={() => act('/api/demo/regression', { enabled: true })}>Inject regression<span>↗</span></button>
            <button className="ghost" disabled={!connected || busy} onClick={() => act('/api/demo/reset')}>Reset demo</button>
          </div>
          {message && <p className="form-error">{message}</p>}<p className="simulation-note">Request traffic and impact figures are simulated. Confluent transport, Flink decisions, and connector delivery are live when cloud mode is configured.</p>
        </section>

        <section className="panel health-card">
          <div className="panel-head chart-heading"><div><p className="eyebrow">LIVE RELEASE HEALTH</p><h3>Stable versus canary</h3><span className="subcopy">Six-second hopping windows · updates every two seconds</span></div><div className="legend"><span><i className="stable-dot" />{snapshot.stable_version}</span><span><i className="canary-dot" />{snapshot.canary_version}</span></div></div>
          <div className="chart-grid-pair">
            <div className="chart-block"><div className="chart-label"><span>Average latency</span><b>{latest?.canary.avg_latency_ms ? `${latest.canary.avg_latency_ms.toFixed(0)} ms canary` : 'No canary traffic'}</b></div><MiniBars points={snapshot.health_history} metric="latency" /><div className="chart-scale"><span>0</span><span>750 ms</span></div></div>
            <div className="chart-block"><div className="chart-label"><span>Error rate</span><b>{latest?.canary.request_count ? `${(latest.canary.error_rate * 100).toFixed(1)}% canary` : 'No canary traffic'}</b></div><MiniBars points={snapshot.health_history} metric="errors" /><div className="chart-scale"><span>0</span><span>25%</span></div></div>
          </div>
          <div className="health-summary"><span><b>{latest ? latest.stable.avg_latency_ms.toFixed(0) : '—'} ms</b> stable now</span><span><b>{canaryPeakLatency.toFixed(0)} ms</b> canary peak</span><span><b>{canaryPeakErrors.toFixed(1)}%</b> error peak</span><span className={postRollback ? 'safe' : ''}><b>{postRollback ? 'ISOLATED' : 'MONITORING'}</b> guardrail</span></div>
        </section>

        <section className="panel decision-card">
          <div><p className="eyebrow">DECISION PATH</p><h3>Trace the event that changed production</h3></div>
          <div className="decision-path">{[
            ['1', 'Request telemetry', `${snapshot.metrics.total_requests.toLocaleString()} events observed`, true],
            ['2', 'Confluent Kafka', 'releaseguard_service_metrics', true],
            ['3', 'Flink SQL', snapshot.latest_decision?.reason || 'Comparing 6-second windows', Boolean(snapshot.latest_decision)],
            ['4', 'Release decision', snapshot.latest_decision ? 'ROLLBACK' : 'Awaiting guardrail breach', Boolean(snapshot.latest_decision)],
            ['5', 'HTTP Sink V2', snapshot.latest_action ? 'Canary 10% → 0%' : 'Ready for authenticated delivery', Boolean(snapshot.latest_action)],
          ].map(([number, title, detail, done], index) => <div className={`decision-step ${done ? 'done' : ''}`} key={String(title)}>{index < 4 && <i className="step-line" />}<span className="step-number">{done ? '✓' : number}</span><div><strong>{title}</strong><p>{detail}</p></div></div>)}</div>
          <div className="decision-id"><span>{snapshot.latest_decision?.decision_id || 'No decision yet'}</span><b>{snapshot.pipeline.mode === 'confluent' ? 'CLOUD' : 'LOCAL TWIN'}</b></div>
        </section>

        <section className="panel timeline-card">
          <div className="panel-head"><div><p className="eyebrow">AUDIT STREAM</p><h3>Live decision timeline</h3></div><span className="stream-label"><i />APPEND ONLY</span></div>
          <div className="timeline-list">{[...snapshot.timeline].reverse().slice(0, 5).map((event) => <div className={`timeline-row timeline-${event.tone}`} key={event.event_id}><span className="event-dot" /><time>{fmtTime(event.occurred_at)}</time><div><strong>{event.title}</strong><p>{event.detail}</p></div><b>{event.event_type.replaceAll('_', ' ')}</b></div>)}</div>
        </section>

        <section className="panel products-card">
          <p className="eyebrow">CONFLUENT CONTROL PLANE</p><h3>One governed path from signal to action</h3>
          <div className="product-flow">{['Kafka', 'Flink', 'Schema Registry', 'HTTP Sink V2', 'Stream Lineage'].map((name, index) => <div key={name}><span>{index + 1}</span><strong>{name}</strong></div>)}</div>
          <p className="product-copy">Every request signal, health window, rollback decision, and applied action stays replayable and auditable.</p>
        </section>
      </div>
      <footer><span>ReleaseGuard · deterministic challenge scenario</span><span>marketplace · AWS us-east-2 · {snapshot.pipeline.mode === 'confluent' ? 'Confluent Cloud' : 'local preview'}</span></footer>
    </div>
  </main>;
}
