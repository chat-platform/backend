## 1. Overview

The system has four logical planes:

- **Edge** — load balancers fronting HTTP and WebSocket traffic.
- **Stateless services** — HTTP API, Message Service, Relay, Notification
  Service, Media Service. They read and write durable state in Postgres /
  Redis / object storage, but hold none in process memory. Any instance
  can serve any request; scale by adding replicas.
- **Stateful service** — WS Gateway. Holds live client sockets in process
  memory. This is the only place where process-local state matters: if a
  node dies, its clients must reconnect, the registry entry expires, and
  sync catches them up. That property drives the non-sticky routing
  decision in `04-api-design.md`.
- **Storage** — Postgres (source of truth), Redis (registry + streams +
  cache), object storage (media).
- **Clients** — mobile/web apps, each device with its own WS connection
  and its own sync cursor.

Live messages flow over WebSocket. Everything durable is written to
Postgres first. Redis is transport and ephemeral state only — never the
source of truth.

Media is uploaded directly from the client to object storage via signed
URLs issued by the HTTP API. The server never proxies media bytes, and
media content is never inspected server-side (it is E2EE-blind). Format
validation happens client-side.

## 2. Component Diagram

```
                         ┌───────────────┐
                         │    Clients    │
                         │ (multi-device)│
                         └───────┬───────┘
                                 │
                    ┌────────────┴──────────┐
                    │                       │
              HTTPS │                  WSS  │
                    ▼                       ▼
            ┌─────────────────┐     ┌─────────────────┐
            │  HTTP LB        │     │     WS LB       │
            │ (active/standby)│     │ (active/standby)│
            └───────┬─────────┘     └───────┬─────────┘
                    │                       │
                    ▼                       ▼
            ┌───────────────┐       ┌───────────────┐
            │  HTTP API     │       │  WS Gateway   │
            │  (stateless)  │       │               │
            └───┬───────┬───┘       └───┬───────┬───┘
                │       │               │       │
                │       │   gRPC        │       │  subscribe
                │       └───────────────┘       │
                │               │               │
                │               ▼               │
                │       ┌───────────────┐       │
                │       │  Message      │       │
                │       │  Service      │       │
                │       └───────┬───────┘       │
                │               │               │
                │               │ write         │
                │               ▼               │
                │           ┌────────┐          │
                │           │Postgres│          │
                │           └───┬────┘          │
                │               │ outbox        │
                │               ▼               │
                │           ┌────────┐          │
                │           │ Relay  │          │
                │           └───┬────┘          │
                │               │               │
                │               ▼               │
                │          ┌─────────┐          │
                │          │  Redis  │◄─────────┘
                │          │ Streams │
                │          │+Registry│
                │          └────┬────┘
                │               │
                │               ├──────────────► Notification Svc
                │               │                (push hints)
                │               │
                │               └──► WS Gateway ──► client
                │                         │                         │       
                │       
                │       
                │       
                │
                └──► (auth, profile, groups, blocks, media, sync)
```

## 3. Planes

### 3.1 Edge
- Two LBs: HTTP and WS. Each has an active/standby pair.
- WS LB is plain L4 (TCP). No sticky routing.

### 3.2 WS Gateway
- Holds live client sockets. One socket per device.
- On connect: validates session, registers connection in Redis
  (`user → {device_id → {ws_node, conn_id}}`), heartbeats.
- On `message.send`: forwards to Message Service over gRPC.
- On delivery: reads from its Redis stream `ws:deliver:{node_id}` and
  writes frames to the matching sockets.
- Stateless within a node beyond the socket itself — all coordination
  goes through Redis.

### 3.3 Message Service (stateless, gRPC)
- Authorizes the send (chat membership, block state).
- Persists `chat_msg` + N `outbox` rows (one per recipient device) in a
  single Postgres transaction.
- Returns ack to the sending gateway.
- Does not deliver. Transport is Relay's job — it reads the outbox rows
  and pushes each to whatever WS node holds that device.
- Handle media also:
  - Issues signed upload/download URLs against object storage.
  - Media metadata (media_id, mime, size, storage keys) is written to
  Postgres by the HTTP API; the binary itself never touches our servers.
  - No server-side inspection of media content. Format validation and
  malware checks happen client-side, consistent with the E2EE model.
- Auth, profile, groups, blocks, media, sync, chat list.
- Stateless, scales horizontally.

### 3.4 Relay (stateless-ish worker)
- Reads outbox rows (`dispatched_at IS NULL`), in batches.
- For each: resolve `device_id → ws_node` via Redis registry.
  - If online → `XADD` to `ws:deliver:{node_id}`.
  - If offline → `XADD` to the notification stream.
- Marks `dispatched_at`.
- Reclaims stuck PEL entries via XAUTOCLAIM on crash.
- Redis is transport; Postgres outbox is truth. If Redis is lost,
  reconnect-drain re-delivers.

### 3.5 Notification Service
- Consumes the notification stream.
- Sends APNs/FCM hints. Never the source of truth for a message.
- Sends **push-to-sync** hints only: a wake signal plus an opaque chat identifier.
  No message content, no sender name, no plaintext metadata — the server is to be
  E2EE-blind(if not now, then in later phase) and must not leak through the push channel either.
- **Coalesces** pushes per (device, chat) within a short window to avoid
  push storms on message bursts and to respect APNs/FCM background-wake
  throttling.
- Never the source of truth for a message.

### 3.6 HTTP API
- Auth, profile, groups, blocks, media, sync, chat list.
- Stateless, scales horizontally.

#### Why no seperate auth service:
For a real WhatsApp, a separate auth service would be the better call — security
isolation (secrets, reduced blast radius), multiple consumers (Facebook, WhatsApp
Pay), bigger teams with dedicated ownership, independent scaling and deployment.
We're not doing that. This project uses one service for auth and the other HTTP
endpoints, media URL handling included. A separate media service is also less
necessary here, since virus scans and similar checks happen client-side. Splitting
services wouldn't teach me much, and the operational overhead isn't worth it. So
they're combined into a single "HTTP API."

## 4. Storage

| Store | Role | Durability |
|---|---|---|
| Postgres | Source of truth: users, chats, messages, outbox, blocks, devices, sessions | Durable, replicated |
| Redis | WS registry, streams (relay→WS, relay→notif), ephemeral cache | Not durable; loss tolerable |
| Object storage | Media blobs | Durable |

## 5. Core Data Flows

### 5.1 Send (1:1)
```
Client → WS Gateway → Message Service (gRPC)
                          │
                          ▼
                  Postgres TX:
                    INSERT chat_msg
                    INSERT outbox × N devices
                          │
                          ▼
                  ack → sender
                          │
                  (async) Relay drains outbox
                          │
                  Redis registry lookup per device
                          │
             ┌────────────┴────────────┐
             ▼                         ▼
        online: XADD             offline: XADD
        ws:deliver:{node}        notif stream
             │                         │
             ▼                         ▼
        WS Gateway → client      Notification Svc → APNs/FCM
```

### 5.2 Reconnect
```
WS lost → client reconnects → WS auth
       → HTTP GET /v1/sync?since=cursor
       → apply outbox rows since cursor
       → resume WS live
```

### 5.3 Receipts
```
Recipient device receives message
  → WS receipt.delivered → server
  → Message Service updates outbox.delivered_at for that device
    → Also updated in `msg_seen_status` table
  → Relay pushes receipt to sender's devices
  #TODO: Need to ensure atleast-once delivery with idempotency
```