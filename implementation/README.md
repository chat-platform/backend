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

Phases 1–3 complete: Message Service persists direct messages, WS Gateway
accepts stub identities and sends, and Relay routes live deliveries through
Redis Streams to recipient sockets. Authentication, reconnect sync, offline
recovery, and retry scans remain future work.

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

Docker Compose for local Postgres, Redis, and the three Phase 1–3 services.

## Components

- [x] Message Service (`services/message-service`, Phase 1)
- [x] WS Gateway (`services/ws-gateway`, Phase 2 + live delivery)
- [x] Relay (`services/relay`, Phase 3)
- [ ] Core API (Phase 4)
- [ ] Notification Service (later)

## Development

From the repo root:

```bash
docker compose up --build -d # Postgres, Redis, Message Service, WS Gateway, Relay
make proto       # generate gRPC stubs into shared/gen
make tidy        # sync Go workspace / modules
```

Postgres migrations and seeds run only when its data volume is first created.
The live demo uses `ws://localhost:8080/ws`. Connect two terminals with
`websocat ws://localhost:8080/ws`, then send the following identity as the
first frame. Alice:

```json
{"type":"identity","id":"alice","ts":0,"payload":{"userId":"11111111-1111-4111-8111-111111111111","deviceId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}}
```

Bob:

```json
{"type":"identity","id":"bob","ts":0,"payload":{"userId":"22222222-2222-4222-8222-222222222222","deviceId":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}}
```

Send this from Alice; Bob receives `message.recv`, and Alice receives
`message.ack`:

```json
{"type":"message.send","id":"request-1","ts":0,"payload":{"chatId":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","clientMsgId":"018f5a1c-7b2d-7abc-8def-0123456789ab","content":"hello"}}
```

The delivered payload includes `senderId` and `clientMsgId` to identify the
original message. Since Core API sync is not implemented yet, an offline
recipient is skipped by Relay and will not receive that message live after
reconnect.

The `proto` target requires `protoc`, `protoc-gen-go`, and
`protoc-gen-go-grpc` on `PATH`. The database migration and dev seed run only
when Postgres initializes a new data volume; `make down` preserves that volume.

Seeded DIRECT chat (Phase 0 fixtures):

| Role  | user_id                                | device_id                              |
|-------|----------------------------------------|----------------------------------------|
| Alice | `11111111-1111-4111-8111-111111111111` | `aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa` |
| Bob   | `22222222-2222-4222-8222-222222222222` | `bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb` |

Chat id: `cccccccc-cccc-4ccc-8ccc-cccccccccccc`

Postgres: `postgres://messenger:messenger@localhost:5432/messenger?sslmode=disable`  
Redis: `localhost:6379`

## Testing

- Unit tests
- Integration tests
- Load tests

## Performance

Document measured results here.

## Known Limitations

Stub identity remains unauthenticated. Relay routes only online direct-message
deliveries; offline routing, retry scans, session validation, and reconnect sync
are deferred to later phases.

## Design vs Implementation

Document cases where the implementation differs from the original
system design and explain why.
