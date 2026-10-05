# Architecture Decisions

## Database

Decision: PostgreSQL as primary database
Status: Accepted
Reason:
- relational data model
- transactions required for message + outbox
- strong consistency for chat membership/message writes
- suitable for current project scope
- not read heavy than writes, so not mysql

### Decision: Redis Streams for relay → WS
Status: Accepted
### Communication method: relay → WS
- Redis streams
    - Status: Accepted

### Communication method: relay → notification service
- Redis streams
    - Status: Accepted

## Other decisions
- MEDIA SCAN:
    - The app (client side) shall perform virus scans on media uploads. Virus scans will not be performed by the backend, as we plan to implement end-to-end encryption (in future).
    - Warning UI — Files that fail checks get marked "suspicious" and the user is warned not to open them.

There shall be ws services.
Need LB to distribute traffic to here
Need backup LB ready

ws servers communicates with message service using GRPC
 message service:
    authorizations for the chat
    do persist msg, and outbox
    reply sync for acknowledgement

relay service
    reads outbox
        find chatId->userIds->ws connections (from redis)
                                    |           =>send msg event to them via (?(note A))
                                    |
                                    if not, =>offline
                                        send to notification service (sync/async?)

WS servers
-----------------
- recieves message, transfer it to msg service
- when recieved events from relay service, it sends to corresponding users's devices via ws

decided to use redis stream. One stream per WS server, ws:deliver:{server_id}

Notification service
----------------------------
decided to use redis stream


Redis:
- WS connection registry
- Redis Streams for relay
- ephemeral/cache data


### WS Connection registry
--------------------------
Decision: Redis
    Status: Accepted
    Details: 
        Key:   ws:conn:{device_id}    (Per-device keys)
        Value: {ws_node, conn_id}
        TTL refreshed on heartbeat; key expires if the Gateway stops
    heartbeating. Explicit DEL on clean disconnect.

    Liveness is the key's TTL — no per-field timestamp, no reaper.
    Device-to-user mapping stays in Postgres (device table); Redis
    only tracks live connections.

Why not a per-user hash:
    A hash per user means every device's heartbeat writes to the same
    key (contention), and per-field TTL isn't universally available
    (HEXPIRE is Redis 7.4+). Per-device keys avoid both: one SET per
    heartbeat, TTL handles cleanup, no reaper.