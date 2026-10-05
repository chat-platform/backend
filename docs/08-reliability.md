# Reliability

How the system behaves when things fail, and what guarantees it makes
under failure. The design doc (`07`) describes the happy path; this doc
covers the rest.

## 1. Availability Target

- **Target availability: 99.999%** (five nines).
- This applies to the *message path* — send and receive. Other surfaces
  (profile edits, group management) can tolerate lower availability.
- Five nines allows ~5.26 minutes of downtime per year. This is an
  aspirational target for a portfolio project; the point is to design
  the failure handling, not to claim the SLA.

## 2. Guarantees

What the system promises, and under what conditions.

| Property | Guarantee | Mechanism |
|---|---|---|
| Message durability | Once acked, never lost | Postgres commit before ack |
| Message delivery | At-least-once to each target device | Outbox + relay re-dispatch |
| Message ordering | Per-device, by `outbox.id` | Sync query `ORDER BY id` |
| Receipt delivery | At-least-once | `IS NULL` idempotent writes |
| Push notification | Best-effort | Not source of truth; sync catches up |
| Presence | Best-effort, eventually consistent | Registry TTL; no durability |

Not promised:

- **Exactly-once delivery.** Not achievable without coordination cost
  that isn't justified. At-least-once + client-side dedup on `outbox.id`
  gives the same *observable* behavior.
- **Ordering across devices.** Each device has its own cursor; there is
  no global order.
- **Delivery to revoked/offline devices past retention.** Undelivered
  messages expire after the retention window (`02` §1.9, `07` §4.5) as a
  storage optimization — a device offline that long is treated as
  abandoned. Dropped, not reconciled.
