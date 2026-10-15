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

Phase 1 complete: Message Service persists idempotent direct messages and
returns acknowledgements over gRPC. Phase 2 WS Gateway implementation is in
progress; its no-auth stub identity path will forward sends and return acks.

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

Docker Compose for local Postgres and Redis.

## Components

- [x] Message Service (`services/message-service`, Phase 1)
- [ ] WS Gateway (`services/ws-gateway`, Phase 2 in progress)
- [ ] Relay (`services/relay`)
- [ ] Core API (Phase 4)
- [ ] Notification Service (later)

## Development

From the repo root:

```bash
make up          # Postgres + Redis (schema + seeds on first start)
make proto       # generate gRPC stubs into shared/gen
make tidy        # sync Go workspace / modules
```

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

Phase 2 uses unauthenticated stub identity and does not deliver messages to
recipients. Relay delivery, session validation, and reconnect sync are deferred
to later phases.

## Design vs Implementation

Document cases where the implementation differs from the original
system design and explain why.
