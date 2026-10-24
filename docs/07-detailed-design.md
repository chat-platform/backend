# Detailed Design

How each component actually does its job: the algorithms, the exact
sequences, and the contracts that the high-level design leaves implicit.

- Component overview and connections → `06-high-level-design.md`
- HTTP/WS API surfaces → `04-api-design.md`
- Schema → `05-data-model.md`
- Failure scenarios → `08-reliability.md`

This doc is about mechanism, not interfaces.

---

## 1. Message Write Path

From WS frame to committed outbox row.

### 1.1 Sequence

```
Client (device A of user X)
    │  WS: message.send { clientMsgId, chatId, content }
    ▼
WS Gateway
    │  validate session, forward over gRPC
    ▼
Message Service
    │  1. authorize (membership, block state, chat type)
    │  2. resolve recipients → recipient devices
    │  3. BEGIN TX
    │       DIRECT: INSERT direct_outbox × N devices
    │       GROUP:  INSERT group_event (1 row)
    │               INSERT group_outbox × N devices
    │     COMMIT
    │  4. return ack
    ▼
WS Gateway
    │  WS: message.ack { clientMsgId, ts }
    ▼
Client
```

### 1.2 Authorization

Checked in Message Service, in this order:

- Session valid (delegated to Auth module).
- Sender is a member of `chat_id`.
- For DIRECT chats: neither side has blocked the other.
- For GROUP chats: block state is ignored.

If any check fails, no writes happen; the error is returned to the
sender's WS Gateway. Block failures are silent to the sender.

### 1.3 Recipient resolution

Fan-out decides how many outbox rows to write. Multi-device echo is
**on** for all of a user's devices **except the originating device**: a
message sent by user X from device A is delivered to X's other devices
(B, C), so they see it in real time and stay in sync, but is **not**
echoed back to A.

- **DIRECT chat**: the peer user's live devices, plus the sender's other
  live devices (excluding the originating device A).
- **GROUP chat**: every member's live devices, excluding the sender's
  originating device A.

The originating device (A) is **excluded** from fan-out. Instead, A
relies on the inline `message.ack` as the sole durability confirmation.
If the ack is lost (WS drop, Gateway crash, network failure), A retries
the send with the same `clientMsgId`. The server's idempotent handler
recognizes the existing `clientMsgId` and **re-returns the same ack**
without writing a duplicate outbox row, allowing A to reconcile its
optimistic state (§7.1).

So N = (peer's live devices) + (sender's other live devices) for a
DIRECT chat, and N = (sum of all members' live devices) − 1 for a GROUP
chat.

### 1.4 The transaction

Single Postgres transaction per message. Order:

1. Resolve recipients and their live devices. (Before transaction begins)
2. BEGIN
   - DIRECT: INSERT direct_outbox × N devices (payload inline)
   - GROUP:  INSERT group_event (1 row)
             INSERT group_outbox × N devices
   COMMIT

The transaction is insert-only. Both outboxes'
timestamps `dispatched_at`, `delivered_at` are set later by
Relay and receipt handlers.

### 1.5 Response and retry

`message.ack { clientMsgId, ts }` — the sender knows the
message is durable. Relay picks up the outbox rows asynchronously; the
sender does not wait for delivery.

Two failure modes, both handled idempotently:

**Transaction fails.** Nothing is written — no outbox
rows. The client retries; on the second attempt the insert succeeds
(if the failure was transient) and the normal path continues.

**Commit succeeds, ack is lost.** The message is durably stored in the outbox, but 
the ack never reaches the client (WS drop, Gateway crash, network
failure). The client retries; the Message Service recognizes the same `(sender_id, client_msg_id)` and
re-returns the same ack without writing duplicate outbox rows.

In both cases the client retries with the same `clientMsgId`. The
unique index is the dedup key (see §9) — one stored message per
`clientMsgId`, regardless of how many times the client sends it.

### 1.6 Edge cases

- **Chat deleted / sender removed mid-flight**: transaction fails on
  membership check inside the TX (re-check under lock).
- **Empty peer set** (sender is the only member):
  write outbox rows only for the sender's other devices (no echo). If the
  sender has one device, zero outbox rows. Valid but unusual — arises
  when a user creates a chat with themselves, or when all other members
  have left a group.

---
## 2. Outbox → Relay → WS Delivery

### 2.1 Relay drain loop

Relay runs as N worker instances. Each instances have either or both of 
these:

```
loop1:
  BEGIN
    rows_direct = SELECT * FROM direct_outbox
      WHERE dispatched_at IS NULL
        AND (claim_expiry IS NULL OR claim_expiry <= now())
      ORDER BY id LIMIT batch_size FOR UPDATE SKIP LOCKED
    UPDATE selected rows SET claim_expiry = now() + claim_lease
  COMMIT

  for each claimed row (outside the transaction):
    connection = registry.lookup(row.device_id)
    if online:
      XADD ws:deliver:{connection.node_id} * <row.payload>
      UPDATE direct_outbox SET dispatched_at = now(), routed_to = 'WS',
        claim_expiry = NULL WHERE id = row.id AND dispatched_at IS NULL
    else:
      # Relay policy: skip offline rows.
      UPDATE direct_outbox SET dispatched_at = now(), routed_to = NULL,
        claim_expiry = NULL WHERE id = row.id AND dispatched_at IS NULL

  # Lookup, publish, or dispatch-update failures leave the row undispatched.
  # Its lease expires and a later poll can claim it again.
```
```
loop2:
  BEGIN
    rows_group  = SELECT * FROM group_outbox
          JOIN group_event ON group_event.event_id = group_outbox.event_id
          WHERE group_outbox.dispatched_at IS NULL
          ORDER BY group_outbox.id LIMIT batch_size FOR UPDATE SKIP LOCKED

    for each row in rows_group:
        ws_node = registry.lookup(row.device_id)
        if ws_node:
            XADD ws:deliver:{ws_node} * <row.event_payload>
            routed_to = 'WS'
        else:
            XADD notif:stream * <row.event_payload>
            routed_to = 'NOTIF'
    UPDATE group_outbox
    SET dispatched_at = now(), routed_to = <WS|NOTIF>
    WHERE id IN (...)

    if rows_group is empty:
        sleep(idle_interval)      # slow down
    else:
        if len(rows_group) < batch_size:
            sleep(short_interval) # batch wasn't full; maybe more coming
        else:
            continue
  COMMIT
```

`FOR UPDATE SKIP LOCKED` lets multiple Relay instances drain in parallel
without contending. `ORDER BY id` preserves insertion order within a
batch; global ordering across batches is not guaranteed.

TODO: Ensure global ordering atleast at ws-gateways.

TODO: decide `batch_size` and poll intervals: `idle_interval` and `short_interval` — target outbox drain lag
under N ms during peak.

### 2.2 Registry lookup

Redis key: `ws:conn:{device_id}` → `{ws_node, conn_id}`, with a TTL
refreshed on heartbeat. Relay looks up the row's `device_id` directly to
find the current `ws_node`. No hash, no per-user aggregation in Redis —
the device list lives in Postgres (`device` table), and Redis only tracks
*live* connections for devices that currently have one.

Race: the device may disconnect between the lookup and the `XADD`. The
event lands in `ws:deliver:{ws_node}` but the socket is gone. The WS
Gateway drops it on delivery. The retry scan (§2.4) picks it up, or the
client's next reconnect-sync catches it (§4). Acceptable.

### 2.3 Streams

- One stream per WS node: `ws:deliver:{node_id}`.
- One stream for notifications: `notif:stream`.
- Consumer group on each: WS nodes consume their own; Notification
  Service consumes `notif:stream`.
- Stream lifecycle: TTL when a WS node is decommissioned (see `06` §8).
TODO: decide this TTL

### 2.4 Retry scan

Relay's initial dispatch is a one-shot: it hands the event to a WS
stream or the notification stream, marks `dispatched_at`, and moves on.
Once marked, it never re-dispatches.

But a row dispatched to a WS stream can still be undelivered: the stream
entry is lost (Redis crash, failover, eviction), the Gateway crashes
before consuming, or the socket silently dies. In these cases, the row
is dispatched but the client never received it.

A separate retry scan finds and re-dispatches these rows:

```
loop:
    rows = SELECT * FROM outbox (do for both direct and group outboxes)
           WHERE routed_to = 'WS'
             AND dispatched_at IS NOT NULL
             AND delivered_at IS NULL
             AND attempt_count < 10
             AND dispatched_at <= now() - interval '30 seconds' * 2^(attempt_count - 1) * (0.8 + random() * 0.4)
             AND (claim_expiry IS NULL OR claim_expiry <= now())
           ORDER BY id
           LIMIT batch_size
           FOR UPDATE SKIP LOCKED

    for each row:
        ws_node = registry.lookup(row.device_id)
        if ws_node:
            increment attempt_count and update dispatched_at
            XADD ws:deliver:{ws_node} * <event>

    sleep(retry_interval)
```

Key points:

- **Only `routed_to = 'WS'`.** Rows routed to `notif:stream` (offline
  devices) are excluded — those recover via sync on reconnect, and
  retrying them would flood the notification stream.
- **Backoff** — the initial WS dispatch is attempt 1. Base retry delays
  are 30s, 60s, 120s, and so on, with independent ±20% jitter and at
  most 10 attempts total.
- **Claiming** — Relay uses a bounded `FOR UPDATE SKIP LOCKED` claim and
  the existing lease so concurrent workers do not claim the same row.
- **Exhaustion** — rows at attempt 10 remain in the outbox for inspection
  and are excluded from automatic retries and age-based cleanup.
- **`delivered_at IS NULL`** — the row wasn't acked by the client.
- **Idempotent re-dispatch.** The client dedupes logical messages on
  `(senderId, clientMsgId)`. A retry that arrives after the message was actually delivered is a
  no-op.

Retry is a correctness mechanism, not just a latency optimization: the
client cannot detect gaps in the global `outbox.id` sequence (it sees
only its own rows, which are non-consecutive), so a lost event would be
silently missing without a server-side redelivery path. The retry scan
is that path.

If the device is offline at retry time, `registry.lookup` returns
nothing and the row is skipped. It will be picked up on the device's
next reconnect-sync.

The retry worker scans every 10 seconds in batches of 100 by default;
both settings are configurable. Eligibility uses `dispatched_at` and
`attempt_count`, so old undelivered events remain eligible after backoff.

### 2.5 Duplicate delivery

Relay is at-least-once by construction. It dispatches the event (`XADD`)
*then* sets `dispatched_at`. If it crashes between those two steps, the
outbox (direct_ or group_) row stays undispatched and is re-dispatched on the next poll — 
so the same event can be sent twice.

The retry scan (§2.4) can also re-dispatch a row that was already
delivered but whose ack was lost — the row appears undelivered until
`delivered_at` is written.

In both cases, the client sees the same event twice. 
Deduplication happens client-side on the row's `id`, relative to that table's cursor.
The client tracks **two cursors** — one for `direct_outbox`, one for `group_outbox`. 
Within each table, the cursor skips anything already applied, so a duplicate 
delivery is a no-op.

Server-side, `dispatched_at` may be set twice for the same row.
Harmless — it's just a timestamp, and the value converges.

The two tables have separate sequences, so `direct_outbox.id = 5` and
`group_outbox.id = 5` are distinct events. Dedup is per-table, not
across tables.

---

## 3. Delivery Receipts and Read State

Receipts are events, not state updates. When a device receives or reads
a message, it sends a `receipt.*` frame. The server writes a RECEIPT
event into `direct_outbox`, addressed to the sender's devices. No
server-side read state — the client aggregates.

### 3.1 Delivery receipt

After applying an event and durably persisting its cursor, a device sends
`receipt.delivered { outboxId }`. The Gateway passes the authenticated
device ID and outbox ID to Message Service. Server:

1. Marks the corresponding outbox row (in direct_outbox or
   group_outbox) as delivered:
     UPDATE <table> SET delivered_at = now()
     WHERE id = ? AND device_id = ? AND delivered_at IS NULL

   Idempotent via `delivered_at IS NULL`.

2. Writes a RECEIPT event into direct_outbox, addressed to the
   sender's devices. This is how the sender learns the message was
   delivered.

### 3.2 Read receipt

Triggered when the user (recipient) opens the chat. The client sends one
`receipt.read` per message it has read, addressed to that message's
sender:

    receipt.read { outboxType, outboxId, readAt }

The server does not store read state: there is no `msg_seen_status`, and
no `read_at` column on the outbox.

Read is represented as a **new event**, not a mutation of the original
delivery row. For each `receipt.read`, the handler writes RECEIPT rows
into `direct_outbox`, addressed to the sender's devices:

- DIRECT chat: one `direct_outbox` row per device of the peer,
  `event_type = RECEIPT`,
  `payload = { senderId, clientMsgId, readAt, readerUserId }`.
- GROUP chat: same — addressed to the original sender, so one
  `direct_outbox` row per device of that user.

Per-message (not a range): the client sends one receipt per message. The
outbox type and ID identify the exact source event. Message Service
checks that the authenticated device was a recipient, then derives the
sender and message ID from the stored outbox row. The sender's devices
apply each receipt to the matching message (identified by
`senderId, clientMsgId`).

If multiple devices of the same reader send receipts for the same
message, the sender's client applies the first and ignores duplicates.

Read state is per-user, aggregated client-side (see `06` §6).

The sender's devices receive the RECEIPT event, apply the first one for
a given message (ignoring duplicates from other devices of the same
reader), and render the read tick.

Read state is per-user, aggregated client-side (see `06` §6).

### 3.3 Propagation to sender

Each receipt write on the recipient side is itself an event that must
reach the sender's devices. Receipts flow through `direct_outbox`, same
as messages: the receipt handler inserts one `direct_outbox` row per
sender device, with `event_type = RECEIPT`. One transport, one cursor,
one sync path — every event (message, receipt, edit, delete) is
delivered the same way.

Note: receipts for group messages also go through `direct_outbox`, not
`group_outbox`. A receipt is addressed to one user (the sender), so it
fits the "fan-out to a single user" shape. See `06` §6.

TODO: revisit if outbox write amplification becomes a problem. See: 11-future-notes.md §1

### 3.4 Idempotency

Receipt processing has two parts: marking the delivery on the row, and
writing a RECEIPT event to the sender.

- **Delivery:**
  1. `UPDATE <outbox table> SET delivered_at = now()
     WHERE sender_id = ? AND client_msg_id = ? AND device_id = ?
     AND delivered_at IS NULL`. Reapplying is a
     no-op because of the `delivered_at IS NULL` guard.
  2. Write a RECEIPT event to `direct_outbox`, addressed to the
     sender's devices.

- **Read:** no state to update. Read is represented solely by writing a
  RECEIPT event to `direct_outbox`, addressed to the sender's devices.

Both cases produce a RECEIPT event to the sender. Duplicates are
handled client-side — the sender's client applies the first receipt for
a message and ignores later ones.

The `IS NULL` guard on delivery covers Relay's at-least-once delivery:
a receipt arriving twice marks `delivered_at` once. The RECEIPT event
itself has no server-side dedup — a retried receipt writes another
RECEIPT row, which the client ignores.

### 3.5 Notification Service consumer group

Notification Service consumes `notif:stream` as a Redis Streams consumer
group. Multiple workers(in notification service) share the stream; each message goes to one worker.
On crash, the dead worker's un-acked messages sit in the group's PEL;
other workers reclaim them via `XAUTOCLAIM` (idle threshold ~60s). This
makes push delivery at-least-once under worker crashes.

---

## 4. Reconnect and Sync

### 4.1 Cursor semantics

Each device has **two cursors**, one per outbox table:

- `direct_cursor` — the highest `direct_outbox.id` the device has applied.
- `group_cursor` — the highest `group_outbox.id` the device has applied.

Both are stored client-side. They are sent on `/v1/sync` as `?since_direct=<direct_cursor>&since_group=<group_cursor>`.

The two tables have **separate sequences**, so `id` is not comparable
across them. `direct_outbox.id = 5` and `group_outbox.id = 5` are
distinct events.

Within each table, the sync query filters by `device_id` and `id >
cursor`, so each cursor is effectively per-device, per-table.

### 4.2 The sync query

Sync runs two queries, one per table:

```
SELECT * FROM direct_outbox
WHERE device_id = ?
  AND id > ?
ORDER BY id
LIMIT page_size

SELECT group_outbox.*, group_event.payload
FROM group_outbox
JOIN group_event ON group_event.event_id = group_outbox.event_id
WHERE group_outbox.device_id = ?
  AND group_outbox.id > ?
ORDER BY group_outbox.id
LIMIT page_size
```

Backing indexes: `direct_outbox (device_id, id)` and
`group_outbox (device_id, id)`.

The client pages each table independently until both are exhausted, then
transitions to live. During the sync, the WS Gateway buffers delivery
frames for this device (see §4.4) — the client is not receiving live
events yet, so nothing races the sync.

The client tracks, per table:
- `since` — the cursor it sends.
- `last_seen` — the highest `id` it has applied so far.

On each page, `last_seen` advances to the max `id` in that page. If the
client crashes mid-sync, it resumes with `since=last_seen`. Pages are
applied in order within each table; a gap between pages is not expected
because the query is `ORDER BY id` and monotonic per table.

### 4.3 Ordering

Within each table, `id` preserves insertion order. For NEW_MESSAGE events, insertion order 
is send order (Message Service writes them in a single TX per message,
but across messages the order is whatever Postgres assigned). For
EDIT and DELETE, the same. So the client sees events in the order the
server processed them, per table.

**Cross-table ordering is not defined — by design.** `direct_outbox.id` and
`group_outbox.id` are separate sequences; a direct event and a group
event have no relative order. This is acceptable: events are applied
per-chat, and a chat is either DIRECT or GROUP, never both. So within a
chat, ordering is preserved (one table, one sequence). Across chats,
ordering doesn't matter.

TODO: cross-check with the edit constraint in `02` — edits originate
from the sending device. If device A edits a message and device B reads
it via sync, does B see the edit before or after the original? Answer:
after, because the edit has a higher `id` in the same outbox table. Good.

### 4.4 Sync vs live handoff

Sequence on reconnect:

1. Client opens WS and authenticates; Gateway registers the socket and buffers live deliveries.
2. Client pages `/v1/sync?since_direct=<cursor>&limit=<n>` over HTTP.
3. Client applies each event, persists its cursor, and sends `receipt.delivered { outboxId }`.
4. After all pages are exhausted, client sends `sync.ready` over WS.
5. Gateway flushes buffered events by outbox ID, then streams live deliveries.

The bounded buffer lives in WS Gateway memory, not Redis. It holds up to
1,000 events or 10 MiB. If either limit is exceeded, Gateway closes the
socket with a retryable code; the client reconnects and syncs from its
last persisted cursor. If Gateway crashes mid-sync, the buffer is
discarded and the client follows the same reconnect flow.

### 4.5 Cursor too old

If `since` predates the retention window (see `02` §1.9, 30 days), some
outbox rows are gone. Both tables use the same retention; each cursor is
checked independently. The server returns whatever rows still exist and
lets the client continue from there.

The client applies the returned events, sets `last_seen` to the highest
`id` received per table, and resumes live. Events older than the retention window
are simply not delivered. No "resync from scratch" signal — the client
does not wipe local state.

---

## 5. Presence

### 5.1 Connection tracking and heartbeat

Each authenticated WebSocket connection receives a unique UUID and a Redis
key `presence:conn:{userUUID}:{connectionId}` with a 60-second TTL. Its ID is
also added to `presence:user:{userUUID}`. The Gateway pings every 20 seconds
and refreshes both the existing device routing registration and presence TTL
only after receiving a valid WebSocket pong.

Redis expiry notifications must be enabled with
`notify-keyspace-events Ex` in deployment configuration. Each Gateway
subscribes to `__keyevent@0__:expired` and filters for the `presence:conn:`
prefix. The user and connection UUIDs are parsed from the expired key name.

### 5.2 Cleanup and online transitions

Graceful WebSocket close and Redis expiry notification call the same
connection-specific cleanup function. It removes only the targeted
connection, removes subscriptions using the reverse index, checks remaining
connection TTL keys, and processes offline if none remain. Duplicate cleanup
is safe. Ordinary Redis operations are used without Lua; races may produce
duplicate or out-of-order updates and are accepted for best-effort presence.

The first observed live connection produces an online update. The last
connection's cleanup records the Gateway's server-side offline-detection time
as `users.last_seen_at` through Core API, updates the Redis cache, and
publishes an offline update. Persistence is attempted once with a conditional
SQL update that prevents older timestamps from replacing newer values.

### 5.3 Presence authorization and queries

Any authenticated user may view any user's presence; shared-chat membership
and block state do not restrict visibility for now. Online state comes from
the Redis user connection set and live TTL keys. Online queries do not read
last-seen data. Offline queries use the Redis last-seen cache and fall back
to Core API on a cache miss.

### 5.4 Subscriptions and delivery

`presence.subscribe` first authorizes the requester and registers the
connection in `presence:subscribers:{targetUserID}` and
`presence:subscriptions:{connectionID}`, then reads and returns a fresh
snapshot. Repeated subscriptions are idempotent. Unsubscribe and connection
cleanup remove both directions of the index.

Each Gateway has one Redis Pub/Sub subscription to `presence:updates`; it
forwards received events only to locally subscribed WebSocket connections.
No per-user or per-client Redis Pub/Sub subscriptions, Streams, acknowledgments,
replay, or retry queues are used. The client refreshes with
`presence.subscribe` every two minutes while the chat is open and unsubscribes
when leaving. Failed or timed-out refreshes hide the indicator until a valid
snapshot or update arrives.

### 5.5 Deferred reliability work

Periodic reconciliation of missed expiry notifications is deferred. There is
no presence verification index or periodic scan in this phase. Comprehensive
Redis failure recovery is also deferred: automatic Redis reconnection does
not guarantee restoration of routing registrations, presence registrations,
Pub/Sub subscriptions, or Stream consumers. Track that as separate work.

---

## 6. Media Flow

- Upload: the authenticated client requests a signed PUT URL from Core API,
  supplying category, byte size, and content type. Core generates a random
  media UUID and signs the object key, exact content length, and content type.
  The client uploads directly to object storage; no callback or database row
  is created.
- Download: the authenticated client presents the media UUID to Core API,
  which signs a short-lived GET URL for that object key. Possession of the
  opaque UUID is the sharing mechanism; media IDs and metadata travel inside
  the encrypted message payload.
- Upload limits: 5 MiB for images/audio, 10 MiB for video, and 15 MiB for
  documents. Thumbnails are generated client-side. The backend does not
  proxy, inspect, or scan media bytes.
- Profile-picture media is outside this phase.

---

## 7. Multi-Device Semantics

### 7.1 Client-side reconciliation

Sending is optimistic: the client renders the message immediately with a
pending state and retries until acknowledged.

- **Ack received**: mark the message as sent (replace optimistic state).
- **Ack lost / not received within timeout**: retry the send with the
  same `clientMsgId`. The server dedupes and re-returns the same ack.
- **Retry exhausted**: mark the message as failed and surface it in the
  UI for manual retry or discard.

Since the originating device is excluded from fan-out (§1.3), the ack is
the only durability signal — the client must never mark a message as
sent without it. If no ack is received, retry with exponential back-off
up to a capped ceiling.

### 7.2 Read state across devices

Read is user-level, aggregated client-side. There is no server-side read
state (`msg_seen_status` was removed).

When a device reads a message, it sends a `receipt.read` frame. The
server writes a RECEIPT event into `direct_outbox`, addressed to the
sender's devices (§3.2, §3.3). The sender's devices render the tick.

The reader's other devices are not informed. They hold their own read
state locally, and reading on one device does not mark the message read
on the others.

TODO: decide whether reading on device A should push a state update to
device B, or whether each device keeps independent read state.

### 7.3 Device removal

Revoke the session, delete the registry entry. Existing outbox rows for
that device are not deleted — they age out via retention (§02 §1.9).
Deleting them isn't necessary; the device won't reconnect with the
revoked session, so the rows are never claimed.
TODO: Deletion can be considered as retention is 30 days.


---

## 8. Block Enforcement

TODO. Sketch:

- Where checked: Message Service on send, at authorization time.
- Cache: per-session LRU of block lists, refreshed on block/unblock.
- Direction: check both. (if A blocked B /& B blocked A)
- Group exception: block applies only to DIRECT chats.

---

## 9. Idempotency and Ordering — Cross-Cutting

Summary table:

| Event | Idempotency key | Where enforced |
|---|---|---|
| message.send | `(sender_id, client_msg_id)` | unique index on outbox tables |
| outbox delivery | outbox `id`s (cursors) | client-side cursor |
| receipt.delivered | `(sender_id, client_msg_id, device_id)` + `IS NULL` | outbox `delivered_at` |
| receipt.read | (no state) | client-side first-receipt-wins |
| XAUTOCLAIM redelivery | outbox `id` | client cursor dedupe |

TODO: any event type not covered.

---

## 10. Client-side Delegations

The server relays events. Several correctness responsibilities live on
the client.

### 10.1 Deduplication

Events may arrive more than once (at-least-once delivery, retry scan).
The client skips any event whose row `id` is ≤ its cursor for that
table:

- `direct_cursor` — highest `direct_outbox.id` applied.
- `group_cursor` — highest `group_outbox.id` applied.

### 10.2 First-receipt-wins

Multiple devices of the same recipient may each send a receipt for the
same message. The sender's client applies the first one received and
ignores the rest. Same rule for delivery and read receipts.

### 10.3 Message identity

No `message_id` exists. Messages are identified by
`(sender_id, client_msg_id)`. The client stores this pair locally so
`EDIT`, `DELETE`, `REACTION`, and `RECEIPT` events can be matched.

### 10.4 Optimistic UI

Send is optimistic: render pending, wait for ack.

- Ack → mark sent.
- No ack → retry with the same `clientMsgId` (server dedupes).
- Retries exhausted → mark failed.

The ack is the only durability signal — the originating device is
excluded from fan-out (§1.3).

### 10.5 Read state

Read is user-level, aggregated client-side. The client marks a message
read on the first RECEIPT event for it.

### 10.6 Multi-device read position

TODO: how a user's own devices learn each other's last-read position
per chat. Undecided.

---

## Open / Deferred

- [ ] Batch size and poll interval for Relay drain.
- [x] Sync buffer location: memory vs Redis.
- [x] Cursor-too-old handling.
- [ ] Group admin events, reactions, edits/deletes propagation.
- [x] Message expiry (30-day undelivered) — where enforced, how surfaced.
- [ ] Push coalescing window size.
- [ ] Presence fan-out: who gets notified on online/offline.
- [ ] Multi-device last-read sync across a user's own devices.
- [x] Receipt message identity: `(sender_id, client_msg_id)`. The server
      resolves the pair to the outbox row. The client doesn't need the
      row `id`.
