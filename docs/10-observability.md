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

| Metric | Source | Why |
|---|---|---|
| Message send rate | Message Service | Load baseline |
| Delivery latency (p50/p95/p99) | Outbox dispatch time | User-visible performance |
| Outbox lag (oldest undispatched) | Relay | Is dispatch keeping up? |
| WS connections (per node, total) | WS Gateway | Capacity, churn |
| Sync rate and latency | Core API | Reconnect load |
| Redis op latency | All Redis clients | Registry/stream health |
| Postgres write latency | Message Service | Durable store health |

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

## 5. Open

- Distributed tracing: not enabled (cost/scale). Revisit if debugging requires it.
- Log aggregation: provider-managed; service logs are structured.