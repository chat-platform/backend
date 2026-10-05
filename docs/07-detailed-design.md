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
    │       INSERT chat_msg
    │       INSERT outbox × N devices
    │     COMMIT
    │  4. return ack
    ▼
WS Gateway
    │  WS: message.ack { clientMsgId, messageId, ts }
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

Fan-out decides how many outbox rows to write. In all chat types,
multi-device echo is **on**: a message sent by user X from device A is
also delivered to X's other devices (B, C), so they see it in real time
and stay in sync.

- **DIRECT chat**: the peer user's devices, plus the sender's own other
  devices.
- **GROUP chat**: every member's devices, plus the sender's own other
  devices.

The sender's originating device (A) is **excluded** — it already has the
message optimistically and doesn't need an echo of its own send.

So N = (peer's live devices) + (sender's other live devices) for a DIRECT
chat, and N = (sum of all members' live devices − sender's sending device)
for a GROUP chat.

### 1.4 The transaction

Single Postgres transaction per message. Order:

1. `INSERT INTO chat_msg (...) RETURNING message_id`
2. `INSERT INTO outbox (message_id, chat_id, recipient_id, device_id,
   sender_id, event_type, payload) VALUES (...)` × N devices

The transaction is insert-only for `chat_msg` and `outbox`. The outbox
timestamps `dispatched_at`, `delivered_at`, `read_at` are set later by
Relay and receipt handlers.

### 1.5 Response and retry

`message.ack { clientMsgId, messageId, ts }` — the sender knows the
message is durable. Relay picks up the outbox rows asynchronously; the
sender does not wait for delivery.

Two failure modes, both handled idempotently:

**Transaction fails.** Nothing is written — no `chat_msg`, no outbox
rows. The client retries; on the second attempt the insert succeeds
(if the failure was transient) and the normal path continues.

**Commit succeeds, ack is lost.** The message is durably stored, but
the ack never reaches the client (WS drop, Gateway crash, network
failure). The client retries; Message Service attempts the insert,
hits the unique index on `(sender_id, client_msg_id)`, catches the
violation, looks up the existing `message_id`, and returns the same ack
as the first attempt.

In both cases the client retries with the same `clientMsgId`. The
unique index is the dedup key (see §10) — one stored message per
`clientMsgId`, regardless of how many times the client sends it.

### 1.6 Edge cases

- **Chat deleted / sender removed mid-flight**: transaction fails on
  membership check inside the TX (re-check under lock).
- **Empty peer set** (sender is the only member): write `chat_msg`,
  write outbox rows only for the sender's other devices (echo). If the
  sender has one device, zero outbox rows. Valid but unusual — arises
  when a user creates a chat with themselves, or when all other members
  have left a group.

---

## 2. Outbox → Relay → WS Delivery

### 2.1 Relay drain loop

Relay runs as N worker instances. Each instance:

```
loop:
    rows = SELECT * FROM outbox
           WHERE dispatched_at IS NULL
           ORDER BY id
           LIMIT batch_size
           FOR UPDATE SKIP LOCKED

    for each row:
        ws_node = registry.lookup(row.device_id)
        if ws_node:
            XADD ws:deliver:{ws_node} * <event>
        else:
            XADD notif:stream * <event>

    UPDATE outbox SET dispatched_at = now() WHERE id IN (...)

    sleep(poll_interval)
```

`FOR UPDATE SKIP LOCKED` lets multiple Relay instances drain in parallel
without contending. `ORDER BY id` preserves insertion order within a
batch; global ordering across batches is not guaranteed.

TODO: Ensure global ordering atleast at ws-gateways.

TODO: decide `batch_size` and `poll_interval` — target outbox drain lag
under N ms during peak.

### 2.2 Registry lookup

Redis key: `ws:conn:{user_id}` → hash of `{device_id → {ws_node,
conn_id, last_heartbeat}}`. Relay reads the hash for the row's
`recipient_id`, finds `row.device_id`, gets `ws_node`.

Race: the device may disconnect between the lookup and the `XADD`. The
event lands in `ws:deliver:{ws_node}` but the socket is gone. The WS
Gateway drops it on delivery. The client's next reconnect-sync will
catch it (§5). Acceptable.

### 2.3 Streams

- One stream per WS node: `ws:deliver:{node_id}`.
- One stream for notifications: `notif:stream`.
- Consumer group on each: WS nodes consume their own; Notification
  Service consumes `notif:stream`.
- Stream lifecycle: TTL when a WS node is decommissioned (see `06` §8).
TODO: decide this TTL

### 2.4 Duplicate delivery

Relay is at-least-once by construction. It dispatches the event (`XADD`)
*then* sets `dispatched_at`. If it crashes between those two steps, the
outbox row stays undispatched and is re-dispatched on the next poll —
so the same event can be sent twice.

Deduplication happens client-side on `outbox.id` (the cursor). The
client's sync cursor already skips anything it has seen, so a duplicate
delivery is a no-op.

Server-side, `dispatched_at` may be set twice for the same row.
Harmless — it's just a timestamp, and the value converges.
