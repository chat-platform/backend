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
- when recieved events from relay service, it sends to corresponding users

note A:
    which to use?
        - kafka is overkill
        - NATS
        - Redis stream
        - Other options?
    need to update status (for marking delivery in outbox)
    ensure idempotency

decided to use redis stream. One stream per WS server, ws:deliver:{server_id}

Notification service
----------------------------
decided to use redis stream


WS registry
--------------------------
Redis Hash per user, fields per connection, 
TTL + heartbeat. 
Explicit HDEL on disconnect.