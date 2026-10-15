## Note: DB/Cache hit patterns

This document focuses only on the cache-first read flow and DB/cache hit patterns.
If there is any deviation from the documented plan in other areas—such as DB
models, writes, outbox, delivery, or downstream operations—it may be unintended
and should be ignored for the purposes of this document.

PostgreSQL is the durable source of truth; Redis is a cache-first optimization.
On a cache miss or Redis failure, read from PostgreSQL and populate the cache.

```text
                        Client sends message
                                 |
                                 v
              Authentication layer: authenticates user + device
                                 |
                                 v
              Read chat version: chat_route_version:{chat_id}
                                 |
                                 v
        Read sender authorization:
        chat_auth:{chat_id}:v{version}:{sender_id}
                                 |
                 +---------------+----------------+
                 |                                |
              Cache HIT                        Cache MISS
                 |                                |
                 |                                v
                 |              Load authorization from PostgreSQL
                 |              - GROUP: membership / can-send rule
                 |              - DIRECT: block status
                 |                                |
                 |                    Cache ALLOW or DENY result
                 |                    with short TTL
                 |                                |
                 +---------------+----------------+
                                 |
                                 v
                    Authorization decision?
                                 |
              +------------------+------------------+
              |                                     |
         DENY_*                                  ALLOW
              |                                     |
              v                                     v
      Reject message                     Read recipient cache:
                                          chat_recipients:{chat_id}:v{version}
                                                     |
                                     +---------------+----------------+
                                     |                                |
                                  Cache HIT                       Cache MISS
                                     |                                |
                                     |                                v
                                     |          Load active recipient devices
                                     |          from PostgreSQL for the chat
                                     |                                |
                                     |                Cache per chat with TTL
                                     |                                |
                                     +---------------+----------------+
                                                     |
                                                     v
                                    Exclude sender's current DeviceID
                                                     |
                                                     v
                                    Build outbox recipient list
                                    (user_id, device_id)
                                                     |
                                                     v
                     PostgreSQL transaction:
                       INSERT outbox_events for recipients
                                                     |
                                                     v
                                    Commit transaction
                                                     |
                                                     v
                         Further operations continue elsewhere
                         (dispatcher, delivery, retries, etc.)
```