# WhatsApp-like Messenger — System Design

## Objective

Design and implement a large-scale messaging system inspired by
WhatsApp, focusing primarily on backend architecture and
distributed-system concepts.

## Current Status

🚧 Development phase — implementation is in progress.

See [implementation/README.md](implementation/README.md) for implementation
details, local setup, and current limitations.

## System Design and Plans

The system design documents and implementation plans are in [`docs/`](docs/):

- [Requirements](docs/01-requirements.md)
- [Scope and assumptions](docs/02-scope-and-assumptions.md)
- [Scale estimation](docs/03-scale-estimation.md)
- [API design](docs/04-api-design.md)
- [Data model](docs/05-data-model.md)
- [High-level design](docs/06-high-level-design.md)
- [Detailed design](docs/07-detailed-design.md)
- [Reliability](docs/08-reliability.md)
- [Security](docs/09-security.md)
- [Observability](docs/10-observability.md)
- [Future revision notes](docs/11-future-revison-notes.md)
- [Topics to learn](docs/12-to-learn-notes.md)
- [Cache plans](docs/13-cache-plans/01-send-msg.md) and [cache TTLs](docs/13-cache-plans/01-TTLs.md)
- [Design decisions](docs/decisions.md)

## Scope

### In Scope
- 1-to-1 messaging
- Group messaging
- Message delivery
- Delivery/read receipts
- Online/offline presence
- Multi-device support
- Media messages
- Push notifications

### Out of Scope (Or For later phases)
- Voice/video calling
- Payments
- Stories
- Status updates
- End-to-end encryption implementation
  (initial design phase)

## Design Process

- [x] Requirements
- [x] Scale estimation
- [x] User flows
- [x] API design
- [x] Data model
- [x] High-level architecture
- [x] Detailed design
- [x] Reliability
- [x] Security
- [x] Observability
- [ ] Capacity planning
- [ ] Implementation
- [ ] Load testing
