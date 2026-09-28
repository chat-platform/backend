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
| Presence events | WebSocket | Tied to socket lifecycle. |
| Reconnect sync | HTTP, then WebSocket | HTTP backfills; WS resumes live. |

## 3. HTTP API
<!-- define HTTP APIs here -->
## 4. WebSocket API
<!-- define ws APIs here -->

## 5. Core Flows
