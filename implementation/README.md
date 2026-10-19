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

Phases 1–3 and Phase 4A are complete. Phase 4B will add reconnect sync and
offline recovery.

## Architecture

- [High-Level Design](../docs/06-high-level-design.md)
- [Detailed Design](../docs/07-detailed-design.md)
- [Data model](../docs/05-data-model.md)
- [API design](../docs/04-api-design.md)

## Technology Stack

### Backend

Go 1.25 or newer, gRPC (WS Gateway → Message Service), WebSocket (clients).

### Database

PostgreSQL (metadata + outbox; source of truth).

### Messaging / Streaming

Redis Streams (Relay → WS Gateway).

### Cache

Redis (WS connection registry).

### Infrastructure

Docker Compose for local Postgres, Redis, Core API, Message Service, WS Gateway,
and Relay.

## Components

- [x] Message Service (`services/message-service`, Phase 1)
- [x] WS Gateway (`services/ws-gateway`, Phase 2 + live delivery)
- [x] Relay (`services/relay`, Phase 3)
- [x] Core API (Phase 4A: Auth & Identity)
- [ ] Reconnect & Sync (Phase 4B)
- [ ] Notification Service (later)

## Development

From the repo root:

```bash
docker compose up --build -d # Postgres, Redis, Core API, Message Service, WS Gateway, Relay
make proto       # generate gRPC stubs into shared/gen
make tidy        # sync Go workspace / modules
```

Postgres migrations and seeds run only when its data volume is first created.
For an existing local volume, apply the new session schema once with:

```sh
docker compose exec -T postgres psql -U messenger -d messenger \
  -f /docker-entrypoint-initdb.d/migrations/000003_core_api_sessions.up.sql
```

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

The delivered payload includes `senderId` and `clientMsgId` to identify the
original message. Phase 4B sync is not implemented yet; offline delivery
recovery remains deferred.

The `proto` target requires `protoc`, `protoc-gen-go`, and
`protoc-gen-go-grpc` on `PATH`. The database migration and dev seed run only
when Postgres initializes a new data volume; `make down` preserves that volume.

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

Relay routes only online direct-message deliveries; offline routing, retry
scans, and reconnect sync are deferred. The development OTP is not suitable
for production authentication.

## Design vs Implementation

Document cases where the implementation differs from the original
system design and explain why.
