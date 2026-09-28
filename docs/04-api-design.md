# 04 — API Design

How clients talk to the backend: which transport carries which
operation, and the flows that matter.

## 1. Protocols

- **HTTP/HTTPS**
- **WebSocket**

Push notifications are a hint only, not the source of truth for a
message.

## 2. Which Transport for What

| Capability | Transport | Why |
|---|---|---|
| Request OTP | HTTP | Request/response; rate-limited. |
| Verify OTP | HTTP | Returns session + device credentials. |
| Create/update profile | HTTP | Durable state; rare event. |
| User lookup by phone | HTTP | On-demand; privacy-sensitive. Not needed for rendering the chat list. |
| Create/update group | HTTP | Transactional; needs immediate ack. Some flows (invite links, join requests) need scope for that. |
| Add/remove group member | HTTP | Authorization-required; immediate response. |
| Add/remove group admin | HTTP | Authorization-heavy; immediate response. |
| Upload media | HTTP | Binary upload; returns media ID. Plan for direct uploads via signed URLs. |
| Send message | WebSocket | Latency-sensitive; frequent; small payloads. |
| Receive message | WebSocket | Server-pushed. |
| Delivery/read receipts (live) | WebSocket | Pushed to peers. |
| Presence events | WebSocket | Tied to socket lifecycle. |
| Typing indicators | WebSocket | Ephemeral. |
| Block / unblock user | HTTP | Mutation; enforced in WS layer. |
| List blocked users | HTTP | Settings query. |
| Reconnect sync | HTTP, then WebSocket | HTTP backfills; WS resumes live. |

## 3. HTTP API
<!-- define HTTP APIs here -->
Conventions:

- Base path: `/v1`
- Auth: `Authorization: Bearer <session-token>`
- Mutations accept `Idempotency-Key` header
- Errors: `{ error: { code, message } }` with proper status codes

### Auth

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/auth/otp/request` | Start OTP flow. |
| `POST` | `/v1/auth/otp/verify` | Verify OTP; returns session token; registers device. |
| `POST` | `/v1/auth/logout` | Invalidate session. |

### Profile

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/profile/me` | Own profile. |
| `PUT` | `/v1/profile/name` | Display name. |
| `PUT` | `/v1/profile/picture` | Profile picture (media ID).  | <!-- Need to change this to direct upload -->
| `GET` | `/v1/users/lookup?phone=...` | Lookup by phone (rate-limited). |

### Groups

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/groups` | Create group. |
| `PATCH` | `/v1/groups/{id}` | Update metadata. |
| `POST` | `/v1/groups/{id}/members` | Add member(s). |
| `DELETE` | `/v1/groups/{id}/members/{userId}` | Remove member. |
| `POST` | `/v1/groups/{id}/admins` | Promote to admin. |
| `DELETE` | `/v1/groups/{id}/admins/{userId}` | Demote admin. |

### Blocking

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/users/{id}/block` | Block. |
| `DELETE` | `/v1/users/{id}/block` | Unblock. |
| `GET` | `/v1/users/me/blocked` | List blocked. |

### Media

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/media` | Upload; returns media ID. |
| `GET` | `/v1/media/{id}` | Download / signed URL. |

### Sync

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/sync?since=<cursor>` | Backfill missed events. |
| `GET` | `/v1/chats` | List chats. |
| `GET` | `/v1/chats/{id}/messages?before=<cursor>` | Paginated history. |

Notes:

- `/auth/otp/verify` also registers the device — one round-trip sets up
  both session and device identity.
- `/profile/picture` stores a media ID from `/v1/media`; the binary
  itself goes through the media endpoint.
- `/sync` is the recovery path when WS frames are missed.

## 4. WebSocket API
<!-- define ws APIs here -->
### Lifecycle

```text
connect → auth (session token) → auth_ok → heartbeat → events
```

- First frame must carry the session token from `/auth/otp/verify`.
- Server pings periodically; missing pongs mark the socket dead.
- Each device opens its own socket. A user is online if any socket
  is live.
- On disconnect, client reconnects and runs the HTTP sync (see 5.3)
  before resuming live delivery.

### Envelope

```json
{
  "type": "message.send" | "message.recv" | "typing" | "presence" | "receipt" | ...,
  "id": "<client-generated id>",
  "ts": 1699999999,
  "payload": { ... }
}
```

### Events

| Event | Direction | Payload | Notes |
|---|---|---|---|
| `message.send` | C → S | `{ chatId, clientMsgId, content }` | Idempotent via `clientMsgId`. |
| `message.ack` | S → C | `{ clientMsgId, messageId, ts }` | Confirms persistence. |
| `message.recv` | S → C | `{ chatId, messageId, senderId, content, ts }` | |
| `receipt.delivered` | S → C | `{ chatId, messageId, userId, ts }` | |
| `receipt.read` | both | `{ chatId, upToMessageId, ts }` | |
| `typing` | both | `{ chatId, state }` | Ephemeral; client-side timeout fallback. |
| `presence` | S → C | `{ userId, status, lastSeen }` | Pushed on change. |
| `group.*` | S → C | `{ groupId, ... }` | Member added, admin promoted, etc. |

Reconnect sync is not a WS event — it goes over HTTP (`/v1/sync`).

Reliability:

- WS is treated as lossy; anything important is recoverable via `/sync`.
- Client sends carry `clientMsgId`; server deduplicates on it.
- Server persists the message, then acks the sender, then fans out.

## 5. Core Flows

### 5.1 Registration

```text
Enter phone number
    ↓
Request OTP          [POST /v1/auth/otp/request]
    ↓
Verify OTP           [POST /v1/auth/otp/verify]  ← session + device creds
    ↓
New user?
    ├── Yes → Create account
    └── No  → Authenticate
    ↓
Register device
    ↓
Set name             [PUT /v1/profile/name]
    ↓
Set picture          [PUT /v1/profile/picture]
```

### 5.2 Send Message

```text
Client
  ↓
WS: message.send { clientMsgId, ... }
  ↓
Server persists
  ↓
WS: message.ack → sender
  ↓
WS: message.recv → recipient's device(s)
  ↓
WS: receipt.delivered / receipt.read
```

### 5.3 Reconnect / Sync

```text
WebSocket lost
    ↓
Reconnect + auth
    ↓
HTTP: GET /v1/sync?since=<cursor>
    ↓
Apply missed events
    ↓
Resume live over WebSocket
```

HTTP first, then WS — subscribing before backfill drops events that
occur during the sync window.

### 5.4 Block / Unblock

Operation is HTTP; enforcement is in the WebSocket layer.

```
POST   /v1/users/{id}/block     → block
DELETE /v1/users/{id}/block     → unblock
GET    /v1/users/me/blocked     → list
```

Enforced across:

- **Messages** — dropped both directions; sender sees no error.
- **Presence** — hidden from each other.
- **Typing & receipts** — suppressed both directions.
- **Group chats** — group messages still flow; the block only applies to direct messages. A blocked user cannot add the blocker to new groups.
- **Media** — new and old media access revoked. Anything already downloaded to a device stays there.

The blocked party is not notified.