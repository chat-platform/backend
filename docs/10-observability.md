# 10 — Observability

Observability provides aggregate signals for system health and operational diagnosis. Service responsibilities and metric boundaries follow the architecture in [Detailed Design](07-detailed-design.md) and [Reliability](08-reliability.md).

## Signals and responsibilities

- **Metrics** describe rates, latency distributions, active work, and failures across service boundaries. They support capacity and reliability analysis, but do not replace durable application state.
- **Structured logs** explain individual operations and failures. They carry safe correlation identifiers so an event can be followed across services.
- **Tracing** is not a default signal. Metrics and logs are the primary diagnostic tools; tracing may be enabled when cross-service timing is needed.

Each service owns metrics for work it performs. A central Prometheus-compatible collector scrapes service metrics endpoints. Metrics endpoints are internal observability interfaces, separate from user-facing APIs. Collection of database- or queue-derived gauges is periodic so scrapes remain independent of dependency latency. A sample reports its success and last-success time; on collection failure, cached values remain available but are visibly stale rather than appearing as fresh zeroes.

## Metrics and interpretation

Counters distinguish request attempts from successful state transitions and provider calls. Histograms record a single well-defined latency boundary. Gauges describe current state or sampled backlog; they do not imply that work completed successfully.

| Service | Signals | Purpose and interpretation |
|---|---|---|
| Core API | HTTP request count and duration by bounded operation, method, and status class | Measures API load and response latency, including sync activity. Route templates, not concrete URLs, define operations. |
| Message Service | Send attempts and durable send latency; first delivery acknowledgement latency | Send latency runs from the send RPC boundary to durable acknowledgement readiness. Replayed idempotent sends are additional attempts. Acknowledgement latency measures event-created-to-first-device-ack for eligible WS-routed message rows. |
| Relay | Outbox pending rows and age; dispatch wait; WS retry backlog, retry age and exhaustion; dispatch and retry outcomes and processing latency | Separates queued work from Relay processing. Pending age follows Relay's lowest-ID processing order and may differ from the row with the earliest creation timestamp. Retry age starts at dispatch and measures time awaiting retry, not end-to-end delivery latency. `relay_retry_exhaustions_total` counts rows when an attempt first reaches the limit; `outbox_ws_retry_exhausted_rows` is the sampled current count of unresolved exhausted rows and can decrease when acknowledgements arrive. Snapshot row counts are capped by `RELAY_SNAPSHOT_COUNT_CAP` (default 1,000); `outbox_snapshot_count_saturated` marks counts that are lower bounds. |
| WS Gateway | Active connections, delivery handling outcomes, and socket write outcomes | Separates Redis-entry handling and Hub acceptance from actual socket writes. Hub acceptance may represent buffering; neither acceptance nor a successful write is a client acknowledgement. |
| Notification Service | Unread stream lag, pending entries, approximate oldest unresolved age, snapshot health, provider calls, token lookup failures, and no-token skips | Unread entries and delivered-but-unacknowledged entries remain distinct. Oldest age considers the first unread entry and oldest pending entry, using Redis-generated stream IDs as approximate enqueue times; bounded metadata lookups can race with concurrent consumption. Provider attempts count actual FCM calls; coalescing multiple hints into one call remains one attempt. FCM acceptance is not device delivery. Token lookup failures and no-token skips are separate from provider calls. |

### Latency boundaries

Three message timings answer different questions:

1. **Durable send latency** measures how long Message Service takes to make a send durably accepted.
2. **Outbox dispatch wait** measures event creation to first successful dispatch. Pending age describes the oldest row in Relay's processing order.
3. **Acknowledged delivery latency** measures event creation to the first client acknowledgement for a recipient-device row initially routed to a live WS path. It includes queue wait, retries, and client receipt delay.

The acknowledgement cohort excludes offline and notification routes, whose acknowledgements may arrive much later through synchronization. Duplicate acknowledgements do not add observations. The timestamp origin is event creation before transaction commit, so this is not an exact commit-to-ack measurement and may include pre-commit time or clock skew.

There is a route-classification race: Relay can publish before the route is durably recorded. A very fast acknowledgement may arrive before the row is classified as WS-routed and therefore be omitted. This is an accepted limitation of the approximate acknowledged-latency distribution.

## Delivery SLO limitations

The acknowledged-latency histogram contains acknowledged rows only. Messages that remain unacknowledged are absent, so the histogram has survivorship bias. A sampled overdue-unacknowledged count provides useful backlog context, but is only a point-in-time gauge. Neither signal alone, nor their combination, provides a delivery SLO compliance percentage.

Accurate compliance requires an eligible-message denominator and explicit outcome accounting over the same measurement window, including messages that never receive acknowledgements and a defined treatment for messages still within the deadline. Until those semantics are defined, the <500 ms normal / <1 s peak targets for the online-at-first-WS-route cohort are latency goals, not a measurable compliance percentage. Offline synchronization remains outside this latency cohort.

## Sampling and cardinality

Outbox and notification queue sizes and ages are sampled because computing them on every scrape would couple metrics collection to database or Redis work. Samples use bounded, non-overlapping work and do not scan queue contents; notification snapshots use consumer-group metadata and only bounded oldest-entry lookups. The unread count is consumer-group lag, which can be unknown; unknown is a collection failure, not zero. Oldest unresolved age is approximate because Redis metadata and entry lookups are not an atomic snapshot, and stream ID timestamps reflect Redis time rather than application creation time. A failed or unknown sample is distinct from an empty queue: preserve the last known values, mark collection unhealthy, and expose sample freshness. Replicas may report the same consumer-group snapshot; aggregate these gauges with `max`, not `sum`.

Metric labels are limited to service, bounded operation templates, outbox type, and small outcome or result enums. Do not label by user, device, chat, message, connection, outbox ID, token, raw URL, payload, or error text. Such values create unbounded cardinality and belong, when safe, in structured logs.

Logs should avoid duplicate reports of the same failure across layers. The layer with operation and protocol context records the failure once; lower layers return contextual errors. Safe correlation fields such as client message ID, outbox table and row ID, device ID, or chat ID can connect an operation across services without becoming metric labels. Never log request bodies, OTPs, credentials, push tokens, or signed URLs.
