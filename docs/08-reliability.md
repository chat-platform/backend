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

## 3. Failure Scenarios

### 3.1 Client disconnects

**Detection:** WS heartbeat timeout (see `07` §5.3). Server marks the
socket dead; registry key expires.

**Impact:** No live delivery to that device. Messages accumulate in the
outbox.

**Recovery:** Client reconnects, runs sync (`07` §4), catches up from
the outbox. Missed messages arrive. No user-visible loss.

### 3.2 WS Gateway crashes

**Detection:** Process death. Its Redis keys expire (TTL). Clients see
their sockets drop.

**Impact:**
- Connections held by the dead node are lost.
- In-flight frames in `ws:deliver:{node_id}` are undelivered.
- Its in-memory sync buffers are discarded.

**Recovery:**
- Clients reconnect to other WS nodes; the LB routes them.
- Reconnect runs the standard sync flow, pulling missed events from the
  outbox.
- The abandoned stream `ws:deliver:{node_id}` is drained by... nobody.
  Its entries are simply lost — but they were never the source of truth;
  the outbox rows they were derived from remain in Postgres regardless
  of `dispatched_at`.

**Why sync is sufficient:** The sync query reads the outbox directly and
filters on `device_id` and `id` — it does not look at `dispatched_at`.
Whether or not an event was dispatched to a stream, sync returns it. The
stream is a best-effort live channel; the outbox is the durable record.

**Cleanup:** The abandoned stream should be TTL'd when the node is
decommissioned (see `07` §2.3).

### 3.3 Relay crashes

**Detection:** Process death. Other Relay workers continue.

**Impact:**
- In-flight batch processing is interrupted. The `FOR UPDATE SKIP LOCKED`
  transaction rolls back; those rows stay `dispatched_at IS NULL`.

**Recovery:** Next Relay poll (any worker) picks up the undispatched
rows. At-least-once means some events may be dispatched twice.

**No data loss:** The outbox rows persist in Postgres. Crashes only
delay dispatch, they don't drop rows.

### 3.4 Message Service crashes mid-write

**Detection:** gRPC call fails or times out.

**Impact:**
- The transaction either commits fully or not at all. No partial writes.
- If it commits but the ack is lost, the client retries (§1.5).

**Recovery:** Client retry with the same `clientMsgId`. Unique index
makes it idempotent. See `07` §1.5.

**No duplicates:** The unique index on `(sender_id, client_msg_id)`
guarantees one stored message per client send attempt.

### 3.5 Postgres primary fails

**Detection:** Health check failure; writes start erroring.

**Impact:**
- Writes blocked (message sends, receipt writes, outbox dispatch).
- Reads may continue from a replica, depending on configuration.

**Recovery:**
- Failover to a replica. This project assumes managed Postgres with
  automatic failover — the provider promotes a replica on primary
  failure. Self-hosted alternatives (Patroni, pg_auto_failover) are
  noted in `12-to-learn-notes.md` §1.
- Downtime is bounded by failover time (~30s typical).
- After failover, writes resume. Clients retry failed sends; idempotency
  handles duplicates.

**Availability impact:** Postgres is the biggest single-point risk.
Five nines requires fast failover. Managed Postgres gives that without
building HA infrastructure; self-hosting it is a topic for later (see
`12-to-learn-notes.md` §1).

### 3.6 Redis fails

**Detection:** Connection errors from clients of Redis.

**Impact:**
- **Registry (`ws:conn:*`):** Lost. No device appears online.
- **Streams (`ws:deliver:*`, `notif:stream`):** In-flight entries lost.
- **Heartbeats:** Fail; keys expire faster.

**Recovery:**
- Registry rebuilds as clients reconnect and re-register.
- Streams: undelivered frames are lost, but the outbox rows they derived
  from are either already dispatched (and will be missed until reconnect-
  sync) or not yet dispatched (and will be re-XADDed on next Relay poll).
- Notification stream: lost pushes are missed; client relies on
  reconnect-sync.

**Key insight:** Redis is transport, never truth. Loss is tolerable
because the outbox is the durable record. This is the design's central
reliability property.

**Operational note:** AOF persistence helps for short outages — the
registry and in-flight stream entries survive a brief restart, so
clients don't all reconnect at once and the registry doesn't rebuild
from zero. Not required for correctness (the outbox is the durable
record); it's about avoiding a recovery storm, not about preventing
loss.



### 3.7 Notification Service crashes

**Detection:** Worker process death.

**Impact:** Push notifications delayed or dropped.

**Recovery:** Other workers reclaim PEL entries (`XAUTOCLAIM`, `07`
§3.5). Push delivery is at-least-once. Missing pushes are caught up by
reconnect-sync; push is only a hint.

**No user-visible impact** for messages — push only accelerates the
client noticing there's something to sync.

### 3.8 Network partition

**Detection:** Varies by component.

**Impact:**
- **Client ↔ WS Gateway:** client reconnects; standard recovery.
- **WS Gateway ↔ Message Service:** sends fail; client retries.
- **Message Service ↔ Postgres:** writes fail; client retries.
- **Relay ↔ Postgres / Redis:** dispatch stalls; resumes on heal.
- **WS Gateway ↔ Redis:** Registry stale — entries degrade as TTLs are
  not refreshed. New connections are accepted but not registered
  (send-only until recovery). Existing connections degrade the same
  way as their entries expire. On Redis recovery, the Gateway
  re-registers its live connections; see `07` §5.6 for the mechanism.
  Sync catches anything a dropped socket missed.


**Recovery:** Most failures resolve through the same pattern — retry,
reconnect, or sync. The `WS Gateway ↔ Redis` partition is the exception:
live sockets need server-driven re-registration on heal (see `07` §5.6).
Any client whose socket *did* drop still recovers via the standard
reconnect + sync path. 

### 3.9 Push provider (APNs / FCM) failure

**Detection:** API error from provider.

**Impact:** Push notifications not delivered.

**Recovery:** Retry with backoff. If persistent, the push is dropped —
it was only a hint, and the client will sync when it next connects.

**No impact on message delivery** — the message is already durable in
the outbox.
