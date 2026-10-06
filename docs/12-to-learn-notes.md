# 11 — To-learn Notes

Topics worth learning, and decisions that may be revisited for the sake
of learning (higher-level or advanced choices). Each entry states the
current choice, what would trigger a revision, and the alternatives —
not just for this project's needs, but as a record of what exists and
how the options compare.

This is a learning log, not an action list. Entries here may never be
acted on; they exist so the reasoning is captured and the alternatives
are understood.

## 1. Postgres Failover

**Current:** Managed Postgres with automatic failover.

**Trigger to revisit:**
- In a real project: moving off managed (cost, portability, control), or
  needing failover behavior the provider doesn't offer.
- Here: learning. Managed Postgres hides failover entirely, which is
  convenient but teaches nothing. The alternatives below are worth
  understanding because they're what most companies actually run when
  they self-host.

**Other options:**

- **Patroni.** Open-source HA for self-hosted Postgres. Consensus via
  etcd/Consul/ZooKeeper, replica promotion on failure, split-brain
  prevention. The de facto standard for self-managed Postgres HA.
  Mature, widely deployed, steep operational learning curve.

- **pg_auto_failover.** Simpler alternative from Citus. Fewer moving
  parts than Patroni, less flexible. Worth comparing against Patroni
  to understand what Patroni adds (and whether it's worth the
  complexity).

- **Multi-primary / distributed Postgres.** CockroachDB, YugabyteDB,
  TiDB. Remove the single primary entirely; each node can accept
  writes. Trades a familiar single-node consistency model for a
  distributed one. Overkill for this project's scale, but worth
  studying as a class of system — it's a fundamentally different
  approach to the same problem.

**Leaning if revisited:** Patroni, if self-hosting. It's the standard,
and understanding it means understanding how Postgres HA actually
works under the hood.

**Learning notes:**
- How does Patroni's leader election work? (etcd, leases, TTLs.)
- What's the difference between RPO and RTO, and how does each option
  affect them?
- When does multi-primary actually pay off vs. a well-run single
  primary? (Spoiler: geography and write locality, not "scale".)


## 2. Multi-Region

**Current:** Out of scope. Single-region deployment assumed throughout.

**Trigger to revisit:** Users in multiple geographies where cross-region
latency is unacceptable, or a product requirement for regional
data residency.

**Why it's hard:** The current design assumes one Postgres primary and
one Redis. Multi-region means either:

- **Active-passive.** One region serves writes; others serve reads
  (stale) or redirect to the primary. Simpler, but fails if the
  primary region goes down.
- **Active-active.** Multiple regions accept writes. Requires solving
  conflict resolution for messages, membership, receipts, and device
  state. Far more complex; most chat systems don't do this at the
  message level.

**Other considerations:**

- **User locality.** Most users talk to other users in the same region,
  so region-pinning a chat (all participants route to one region) is a
  common approach. Simpler than global consistency.
- **Outbox and sync.** The cursor is per-device and global. In
  multi-region, the cursor's source (the outbox) would need to be
  either replicated (consistency questions) or region-local (breaks
  cross-region chats).
- **Redis registry.** Also would need per-region or replicated setup;
  the registry is on the delivery hot path.

**Leaning if revisited:** Region-pinning by chat, not active-active. Most
chat traffic is local, and pinning avoids the hardest consistency
problems while still reducing latency.

**Learning notes:**
- How do systems like Discord, Slack, or Matrix handle multi-region?
- What is "home region" pinning, and how does it interact with user
  mobility?
- Consistency models: eventual, causal, strong — which fits chat?

## 3. Distributed Tracing

**Current:** Not instrumented. Rely on metrics + logs; add tracing
temporarily when investigating an incident.

**Trigger to revisit:**
- In a real project: debugging gets hard — a latency regression that
  spans services, or an incident that logs alone can't reconstruct.
- Here: learning. Tracing is a standard observability tool that's
  currently absent from the design. Worth understanding what it would
  look like and when it pays off.

**Other options:**

- **Head sampling.** Decide at request start whether to trace (e.g.
  0.1%). Cheap, but errors and slow paths get diluted.
- **Tail sampling.** Buffer spans, decide after the trace finishes based
  on outcome. Keeps interesting traces, needs a buffering collector.
- **On-demand tracing.** Off by default; enable for a specific user,
  chat, or window during debugging. Requires instrumentation to already
  exist in code, gated by config.

**Leaning if revisited:** On-demand tracing. Near-zero steady-state
cost, on exactly when needed. Upfront cost is span code in every service.

**Learning notes:**
- How does a trace cross async hops (outbox → relay → stream → WS)?
  Standard tracing assumes a call chain, not an event chain.
- Head vs. tail sampling: cost, complexity, what each misses.
- OpenTelemetry: what it standardizes, what it doesn't.
- What does a trace look like for a message send? How many spans, which
  services?