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
             AND dispatched_at < now() - retry_threshold
             AND dispatched_at > now() - retry_window
           ORDER BY id
           LIMIT batch_size
           FOR UPDATE SKIP LOCKED

    for each row:
        ws_node = registry.lookup(row.device_id)
        if ws_node:
            XADD ws:deliver:{ws_node} * <event>

    sleep(retry_interval)
```

Key points:

- **Only `routed_to = 'WS'`.** Rows routed to `notif:stream` (offline
  devices) are excluded — those recover via sync on reconnect, and
  retrying them would flood the notification stream.
- **`retry_threshold`** — how long a row must be dispatched-and-
  undelivered before it's considered stuck. Must exceed the normal
  delivery round-trip (Gateway send → client ack → Message Service →
  `delivered_at`), otherwise healthy in-flight rows get retried.
- **`retry_window`** — how far back the scan looks. Rows dispatched
  longer ago than this are outside the window; they're either
  abandoned or handled by another mechanism.
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

TODO: decide `retry_threshold`, `retry_window`, `retry_interval`, and
the scan's `batch_size`.

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

    receipt.read { chatId, senderId, clientMsgId, readAt }

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
server does not need to know the chat's message order, and cannot verify
a range — it just forwards each receipt. The sender's devices apply each
receipt to the matching message (identified by `senderId, clientMsgId`).

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

Direct-message sync uses one persisted cursor per device:

- `direct_cursor` — the highest `direct_outbox.id` the device has applied.
- Group sync and `group_cursor` are deferred until group outbox support exists.

`direct_cursor` is stored client-side and sent as
`/v1/sync?since_direct=<direct_cursor>&limit=<n>`. The default page size
is 100; the server caps it at 500.

### 4.2 The sync query

Sync runs this query for the authenticated device:

```
SELECT id, event_type, payload FROM direct_outbox
WHERE device_id = ?
  AND id > ?
ORDER BY id
LIMIT page_size
```

The existing `direct_outbox (device_id, id)` index supports this query.

The client pages direct events until `hasMore` is false, then transitions
to live. During sync, the WS Gateway buffers live delivery frames for the
connection (see §4.4).

The client advances and persists `direct_cursor` only after durably
applying each event. If it crashes mid-sync, it resumes from its last
persisted cursor.

### 4.3 Ordering

The design retains the existing global `direct_outbox.id` sequence. Concurrent
transactions can commit out of ID order, and Relay workers can publish in
parallel. Therefore applying a higher ID before an unseen lower ID can cause
the client cursor to skip that lower event; strict no-loss ordering is not
guaranteed.

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

If `since_direct` predates retention, expired undelivered rows may be
gone. The server returns remaining rows and lets the client continue from
the last ID it received. Delivered rows are removed by Relay's daily
cleanup; undelivered rows are deleted after 30 days.

No full-resync signal is returned and the client does not wipe local state.

---

## 5. Presence

### 5.1 Registry

Each live device has a Redis key:

    ws:conn:{device_id} → {ws_node, conn_id}

TTL is refreshed on every heartbeat. A clean disconnect deletes the key.
An unclean one lets it expire.

Liveness = key exists. There is no per-user structure in Redis; the
device list comes from Postgres (`device` table), and Redis only says
which of those devices currently have a live socket.

### 5.2 Online / offline

- **Online** = the user has at least one device whose `ws:conn:{device_id}`
  key exists.
- **Last seen** = `max(device.last_seen_at)` across the user's devices,
  read from Postgres. Updated on disconnect (and periodically during a
  long-lived connection).

### 5.3 Heartbeat

Gateway pings; client pongs; missed pongs mark the socket dead.
TODO. Decide cadence (e.g. 30s) and TTL (e.g. 90s = 3× cadence).

### 5.4 Presence fan-out

TODO. Who gets notified when a user goes online/offline:
- Only contacts with an open chat?
- Only contacts currently subscribed to presence?
- Debounce: a user flipping between networks shouldn't spam contacts.

### 5.5 Write amplification

Presence changes are frequent. Pushing every flip live to every contact
is expensive. TODO: batch or debounce — e.g. coalesce per-contact
presence updates over a short window before fan-out.

### 5.6 Registration failures and recovery

The Gateway writes `ws:conn:{device_id}` on connect. If the write fails
(Redis unreachable(network partition)), the connection is still accepted — a device that
can send is better than one that can neither send nor connect. It
degrades to send-only until registration succeeds.

**Queue.** Each Gateway keeps an in-memory queue of connections whose
registry write failed or is pending. In-memory is sufficient: if the
Gateway crashes, the sockets die with it, and the entries become
meaningless.

**Re-registration on Redis recovery.** When Redis becomes reachable,
the Gateway drains the queue. For each connection:

1. Ping the client, wait for a pong within the heartbeat timeout.
2. On pong: write `ws:conn:{device_id}`. The connection becomes routable.
3. On timeout: drop the socket. The client's next reconnect handles it.

**Why the ping-pong.** During a Redis partition, a client connected to
Gateway A (unregistered) may also reconnect to Gateway B (registered)
once its socket to A drops. If Gateway A later re-registers without
checking, it overwrites B's entry with a stale `ws_node` — a phantom
routing target. The ping-pong confirms the client is still on A before
writing.

**Reuse the heartbeat ping.** The check reuses the existing heartbeat
ping/pong — no new frame type needed.

**Stagger the drain.** At scale, draining every Gateway's queue at once
produces a Redis write spike on recovery. Stagger the drain (random
offset within a window) or rate-limit writes per Gateway.

---

## 6. Media Flow

TODO. Sketch:

- Upload: Core signs URL → client uploads direct → client calls back
  with media_id → Core writes `media` row.
- Download: Core signs URL → client fetches from object storage.
- Thumbnails: generated client-side (E2EE constraint).
- Association: - Association: media IDs are carried in the message payload (E2EE).
  The server does not store message-media associations.

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
The client skips any event whose row `id` is ≤ its direct cursor:

- `direct_cursor` — highest `direct_outbox.id` durably applied.

The client persists the event and cursor before sending
`receipt.delivered { outboxId }`; Relay may then remove the row during its
next daily cleanup run.

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
