# 11 — Future Revision Notes

Decisions that may be revisited. Each entry states the current choice,
the alternatives, and the trigger that would cause a revision.

---

## 1. Read Receipts: Propagation to Sender

**Current approach:** Receipts flow through the outbox, same as messages.
The receipt handler inserts an outbox row per sender device
(`event_type = RECEIPT`). One transport, one cursor, one sync path — every
event (message, receipt, edit, delete) is delivered the same way.

**Why:** Uniformity. The sender's devices learn about receipts through the
same sync cursor that catches them up on messages. Offline senders get
receipts on reconnect without a second delivery mechanism. Redis loss or
a WS Gateway crash doesn't lose receipts — the outbox row stays
undispatched and gets picked up.

**Trigger to revisit:** Outbox write amplification. In a busy chat,
receipts, edits, and deletes can outnumber message rows. If receipt rows
dominate outbox volume and Relay drain becomes a bottleneck, consider
one of the alternatives below.

**Other options:**

- **Direct WS delivery.** Receipt handler XADDs straight to the sender's
  `ws:deliver:{node}` streams, bypassing outbox. Faster, but only works
  when the sender is online and the node is known. No recovery on Redis
  loss or Gateway crash. Receipts to offline senders would be dropped
  unless a second durable path is added — which reintroduces the outbox.

- **Ephemeral receipt channel.** Receipts ride a separate best-effort
  path (e.g. a lightweight pub/sub), not durable. Loses receipts for
  offline senders by design; UI reconciles on next message or sync.
  Acceptable only if the product treats receipts as soft state.

- **Dedicated receipts table.** Receipts stored in their own table with
  their own transport, separate from the outbox. Isolates volume, but
  duplicates the cursor, sync, and delivery machinery the outbox
  already provides.

**Leaning if revisited:** Direct WS delivery for the online fast path,
falling back to outbox for offline senders. Combines low latency with
durability, at the cost of two paths to reason about.

---

## 2. WebSocket Gateway Scalability: Go vs. Erlang/OTP

**Current approach:** Go WebSocket Gateway using one goroutine per connection, with per-connection heartbeat handling and Redis-backed connection registration.

**Why:** Go provides lightweight concurrency, efficient networking, and straightforward integration with the existing backend. The current implementation is a reasonable baseline for learning and benchmarking high-concurrency connection management.

**Trigger to revisit:** Before considering the gateway design complete, benchmark it under progressively increasing concurrent connection counts and identify bottlenecks in memory usage, heartbeat traffic, Redis operations, CPU utilization, and connection stability.

**Other options:**

- **Optimized Go gateway.** Improve connection memory usage, heartbeat scheduling, registry refresh frequency, and network I/O based on profiling results. Explore centralized heartbeat scheduling if per-connection tickers become a measurable overhead.
- **Erlang/OTP gateway.** Build a separate prototype using BEAM processes, message passing, and supervision. Compare connection density, memory usage, failure isolation, and operational complexity against Go under equivalent workloads.
- **Hybrid architecture.** Retain Go for messaging and business logic while using Erlang/OTP for WebSocket connection management, if benchmarks demonstrate a meaningful advantage.

**Leaning if revisited:** Keep Go as the primary implementation and use it as the baseline. Explore Erlang/OTP as a comparative experiment to understand runtime-level trade-offs at high concurrency, rather than assuming either language is inherently more scalable. Document benchmark conditions, results, bottlenecks, and trade-offs.