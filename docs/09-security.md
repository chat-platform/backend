# Security

Security properties, controls, and the threat model this design assumes.

## 1. Threat Model

- **Assumed adversary**: network eavesdropper, malicious client, compromised infrastructure component.
- **Not defended against:** 
    - **Server compromise.** Content is E2EE-protected, but the server
    still sees metadata: who talks to whom, when, message sizes,
    delivery state. E2EE protects content, not metadata.
    - compromised client devices (out of scope — client security is the client's responsibility).
- **Trust boundaries**: client (untrusted), edge (LB), services (trusted), storage (trusted).

## 2. Authentication

- OTP flow: request → verify → session + device registration.
- **Rate limiting:**
  - **OTP request:** per phone number (e.g. 3/10min), per IP (e.g. 10/hour), per device fingerprint (if available).
  - **OTP verify:** per code (max guesses), per IP (prevent brute force across numbers).
  - **API (post-auth):** per session/user for message sends and other mutations.
- Session model: access token (short-lived) + refresh token (per session, revocable).
- Device revocation: session revoked, registry entry deleted.

Rate limiting is enforced **in the services**, not the LB. The LB
forwards connections; it doesn't have the identity or the business
context to apply product-level limits.

The LB may apply coarse connection-level limits (max connections per
IP, max connection rate) as flood protection, but not product-level
rate limits. Those live in the services.

TBD: Rate-limits

## 3. Authorization

- Message Service checks: session valid, sender is chat member, block state (DIRECT only).
- Group membership: checked at send time.
- Media: signed URLs are the access control.
- Receipts: a device can only ack messages it has received (checked
  against the outbox row for its device).

## 4. Encryption

### In transit

- HTTPS for HTTP API, WSS for WebSocket.
- TLS terminates at the LB. The LB is Layer 4 + TLS termination.
- Central certificate management: Certs are on the LBs only, not on
  every backend node. Lower operational overhead, lower risk of
  expiry-driven outages.
- Traffic between the LB and backends stays on a private network.

### At rest
- Postgres: provider-managed disk encryption.
- Redis: AOF/RDB files encrypted at the filesystem level.
  In-memory data is not encrypted; encryption at rest protects only 
  the persisted files, not the running process.
- Object storage: SSE (server-side encryption).

### E2EE (later phase)
- The server does not store messages; the outbox payload is transient
  transport only.
- When E2EE lands, the payload carries ciphertext. The server forwards
  it without reading.
- No server-side content inspection; validation is client-side.

## 5. Abuse Prevention

- **Behavioral detection:** server-side signals from metadata only (the
  server is content-blind): send rate, recipient count, new-contact
  ratio, report rate. No content inspection.
- **Non-contact message handling:** media from unknown senders is blurred
  by default, and links are disabled until the recipient chooses to
  interact. This limits the blast radius of unsolicited messages without
  server-side content inspection.
- **Account bans:** proactive, before user reports.

## 6. Data Protection

- Phone numbers: lookup is rate-limited and privacy-sensitive.
TODO: Decide ratelimit for the above
- Media: time-limited signed URLs; revoked for blocked pairs (future signed URLs).
TODO: Decide timelimit for the above
- Outbox: carries the transport payload (message content included), and
  is transient. Aged out after delivery (direct/group); the retention
  window applies as a ceiling for undelivered rows.

## 7. Secrets

- Server holds no message decryption keys (E2EE-blind).
- Infrastructure secrets: DB credentials, Redis auth, media URL signing key. Managed via environment/secret store.

## 8. Open

- Rate limit thresholds: TBD. See §2
- E2EE implementation (later phase).
- Abuse-detection signals: which metadata to track.