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