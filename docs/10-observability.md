# 10 — Observability

How we know the system is healthy, and how we detect when it isn't.

## 1. Signals

- **Metrics** — counters, gauges, histograms. For dashboards and alerts.
- **Logs** — structured, per-service. For debugging.
- **Traces** — not instrumented. At message-send volume, the cost (span
  collection, storage, per-service overhead) isn't justified. Rely on
  metrics + logs; enable tracing temporarily when investigating an
  incident.

## 2. Core Metrics

| Metric | Source | Why | Interpretation |
|---|---|---|---|
| Message send rate | Message Service | Load baseline | Drops signal outage; spikes signal surge |
| Delivery latency (p50/p95/p99) | Outbox dispatch time | User-visible performance | Wide p50–p99 gap signals a slow subset (device, region) |
| Relay lag (oldest undispatched) | Relay | Is dispatch keeping up? | Rising = Relay can't keep up; flat = healthy |
| Retry lag (oldest dispatched-but-undelivered WS row) | Retry scan | Are lost messages being redelivered? | Rising = messages stuck; correctness signal |
| WS connections (per node, total) | WS Gateway | Capacity and churn | Per-node for capacity; total for churn |
| Sync rate and latency | Core API | Reconnect load | Spike after an incident = mass reconnect |
| Redis op latency | All Redis clients | Registry/stream health | Rising = Redis under pressure |
| Postgres write latency | Message Service | Durable store health | Rising = write path degradation |

## 3. SLOs

From `03-scale-estimation.md`:

- Message delivery latency: <500 ms normal, <1 s peak.
- API response latency: <200 ms.

## 4. Alerts

Symptom-based, not cause-based:

- **Relay lag** (oldest undispatched row) > threshold for X min — dispatch is falling behind.
- **Retry lag** (oldest dispatched-but-undelivered WS row) > threshold for Y min — lost messages aren't being redelivered. Correctness signal, not just latency.
- **Delivery latency p99** > SLO for 5 min.
- **WS connection count** drops > X% in 5 min.
- **Postgres/Redis error rate** > threshold.

TBD: Thresholds — tune after load testing.

## 5. Correlation

Every log line and metric carries identifiers that tie it to the
client's action:

- `clientMsgId` — at message send.
- `outbox.id` — for any outbox event (dispatch, delivery, retry).
- `device_id` — the target device.
- `chat_id` — the chat, when relevant.

This lets one client action (send) be tied to its server-side events
(dispatch, delivery, retry) by searching for the same id. Without a
shared key, correlating across services is guesswork.

Note: `device_id` and `chat_id` are useful for logs, but as metric
labels they're high-cardinality — avoid them there. Metrics should use
coarse labels (`service`, `operation`, `region`), not per-entity ones.

## Open:

- Distributed tracing: not enabled (cost/scale). Revisit if debugging requires it.
- Log aggregation: provider-managed; service logs are structured.