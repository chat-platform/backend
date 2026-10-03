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
                │       └───┬───────┬───┘       │
                │           │       │           │
                │           │       │ write     │
                │           │       ▼           │
                │           │   ┌────────┐      │
                │           │   │Postgres│      │
                │           │   └───┬────┘      │
                │           │       │ outbox    │
                │           │       ▼           │
                │           │   ┌────────┐      │
                │           │   │ Relay  │      │
                │           │   └───┬────┘      │
                │           │       │           │
                │           │       ▼           │
                │           │  ┌─────────┐      │
                │           │  │  Redis  │◄─────┘
                │           │  │ Streams │
                │           │  │+Registry│
                │           │  └────┬────┘
                │           │       │
                │           │       ├──────────────► Notification Svc
                │           │       │                (push hints)
                │           │       │
                │           │       └──► WS Gateway ──► client
                │           │
                │           ▼
                │       ┌────────┐
                │       │ Media  │──► Object Storage
                │       │ Service│
                │       └────────┘
                │
                └──► (auth, profile, groups, blocks, media, sync)
```
