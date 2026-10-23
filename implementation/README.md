# Implementation

This directory holds notes on the WhatsApp-like messaging system
implementation. Service code lives in `services/`; shared contracts and
schema live in `shared/`.

## Goals

- Implement the core messaging flow
- Validate the architectural decisions made during system design
- Measure performance and scalability
- Identify gaps between the theoretical design and implementation

## Current Status

Phases 1–3 and Phase 4A are complete. Phase 4B provides direct-message
reconnect sync and offline recovery. Backend online/offline presence is
implemented in the WS Gateway with best-effort Redis Pub/Sub delivery.

## Architecture

- [High-Level Design](../docs/06-high-level-design.md)
- [Detailed Design](../docs/07-detailed-design.md)
- [Data model](../docs/05-data-model.md)
- [API design](../docs/04-api-design.md)

## Technology Stack

### Backend

Go 1.26, gRPC (WS Gateway → Message Service), WebSocket (clients).

### Database

PostgreSQL (metadata + outbox; source of truth).

### Messaging / Streaming

Redis Streams (Relay → WS Gateway and Notification Service).

### Push Notifications

Firebase Cloud Messaging (FCM), using silent data-only push-to-sync hints.

### Cache

Redis (WS connection registry, presence indexes, Pub/Sub, Streams, and cache).

### Infrastructure

Docker Compose for local Postgres, Redis, Core API, Message Service, WS Gateway,
Relay, and Notification Service.

## Components

- [x] Message Service (`services/message-service`, Phase 1)
- [x] WS Gateway (`services/ws-gateway`, Phase 2 + live delivery)
- [x] Relay (`services/relay`, Phase 3)
- [x] Core API (Phase 4A: Auth & Identity)
- [x] Reconnect & Sync (Phase 4B, direct messages)
- [x] Notification Service (FCM push-to-sync hints)

## Development

From the repo root:

```bash
# Keep the service-account key outside the repository.
export GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/firebase-service-account.json"
export GOOGLE_CLOUD_PROJECT="your-firebase-project-id" # optional if included in the credential file
docker compose up --build -d # Postgres, Redis, Core API, Message Service, WS Gateway, Relay, Notification Service
make proto       # generate gRPC stubs into shared/gen
make tidy        # sync Go workspace / modules
```

The Notification Service fails at startup if Firebase Application Default
Credentials are unavailable. Compose mounts the file named by
`GOOGLE_APPLICATION_CREDENTIALS` read-only at the container's ADC path. Do not
commit service-account credentials. Clients register their FCM registration
token using the authenticated `PUT /v1/devices/push-token` endpoint. Pushes
contain only `type=sync` and an opaque `chatId`; clients fetch message data from
the sync API. The Firebase project must have APNs configured for iOS, and web
clients must be configured for Firebase Cloud Messaging.

The baseline migration at `shared/migrations/000001_init.up.sql` creates the
initial schema. Incremental migrations, including
`shared/migrations/000002_presence.up.sql`, are applied in filename order when
Compose initializes a new Postgres data volume. Existing databases must apply
new incremental migrations separately; never reapply the baseline migration
to an existing database.

The baseline is not an incremental migration and must not be applied to an
existing database. Existing databases created with an earlier migration set
already have their own schema and data; preserve those volumes. To recreate a
local development database from the current baseline, first make sure its data
can be discarded, then reset the Compose volume:

```sh
docker compose down -v
docker compose up --build -d
```

The `down -v` command permanently deletes the Compose-managed database volume.

Core API listens on `http://localhost:8081`. Request and verify a development
OTP (`000000`) for each phone number:

```sh
curl -X POST http://localhost:8081/v1/auth/otp/request \
  -H 'Content-Type: application/json' -d '{"phone":"+10000000001"}'
curl -X POST http://localhost:8081/v1/auth/otp/verify \
  -H 'Content-Type: application/json' \
  -d '{"phone":"+10000000001","code":"000000","platform":"WEB","deviceName":"Alice Web"}'
```

Use the returned `token` as the first frame on `ws://localhost:8080/ws`.
For Alice:

```json
{"type":"auth","id":"alice","ts":0,"payload":{"token":"<Alice session token>"}}
```

Bob:

```json
{"type":"auth","id":"bob","ts":0,"payload":{"token":"<Bob session token>"}}
```

Send this from Alice; Bob receives `message.recv`, and Alice receives
`message.ack`:

```json
{"type":"message.send","id":"request-1","ts":0,"payload":{"chatId":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","clientMsgId":"018f5a1c-7b2d-7abc-8def-0123456789ab","content":"hello"}}
```

The delivered payload includes `outboxId`, `senderId`, and `clientMsgId`.
Persist each applied event and its `outboxId` cursor before sending
`receipt.delivered`. On reconnect, open and authenticate the WebSocket, fetch
all pages from `GET /v1/sync?since_direct=<cursor>`, then send `sync.ready`.
The Gateway buffers live deliveries until that frame. Group sync remains
deferred.

The `proto` target requires `protoc`, `protoc-gen-go`, and
`protoc-gen-go-grpc` on `PATH`. The database migration and dev seed run only
when Postgres initializes a new data volume; `make down` preserves that volume.

Redis key expiry notifications are required for presence cleanup. Compose
starts Redis with `notify-keyspace-events Ex`. When using an externally managed
or locally running Redis, configure the server with the equivalent setting and
verify it with `redis-cli CONFIG GET notify-keyspace-events`. The Gateway
listens to `__keyevent@0__:expired` and filters for `presence:conn:` keys.

After WebSocket authentication, clients can send
`presence.subscribe` with `{ "targetUserId": "<user UUID>" }` to receive a
`presence.snapshot` and subsequent `presence.update` frames. Refresh every two
minutes while the chat is open by sending another `presence.subscribe`; send
`presence.unsubscribe` when leaving. A failed or timed-out refresh should hide
the indicator until a valid snapshot or update arrives.

Seeded DIRECT chat (Phase 0 fixtures):

| Role  | Phone          | User ID                                | Seed device ID                         |
|-------|----------------|----------------------------------------|----------------------------------------|
| Alice | `+10000000001` | `11111111-1111-4111-8111-111111111111` | `aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa` |
| Bob   | `+10000000002` | `22222222-2222-4222-8222-222222222222` | `bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb` |

The seeded chat id is `cccccccc-cccc-4ccc-8ccc-cccccccccccc`. Create or
retrieve a direct chat with `POST /v1/chats/direct` using the peer's phone
number and bearer token. For example:

```sh
curl -X POST http://localhost:8081/v1/chats/direct \
  -H 'Authorization: Bearer <Alice session token>' \
  -H 'Content-Type: application/json' -d '{"phone":"+10000000002"}'
```

Postgres: `postgres://messenger:messenger@localhost:5432/messenger?sslmode=disable`  
Redis: `localhost:6379`

## Testing

- Unit tests
- Integration tests
- Load tests

## Performance

Document measured results here.

## Known Limitations

Relay routes only online direct-message deliveries. Offline recovery uses
the direct outbox sync endpoint. The current global outbox ID cursor and
parallel Relay delivery do not guarantee strict no-loss event ordering.
The development OTP is not suitable for production authentication.

## Design vs Implementation

Document cases where the implementation differs from the original
system design and explain why.
