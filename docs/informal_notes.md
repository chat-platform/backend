"I'm choosing Redis Streams for relay → WS gateway and relay → notification service"
Fine choice, but review these:

Pros:

Consumer groups give you at-least-once delivery with per-consumer ack.

Backpressure is natural (stream length, pending entries list).

Decouples relay from gateway count and gateway churn.

Cons / things to get right:

Redis Streams is not durable by default unless you configure AOF/RDB persistence properly. If Redis dies, in-flight events are lost. For a chat system where the durable record is delivery_events, that's tolerable — because reconnect-drain will re-deliver anything not acked. But make sure you're not relying on the stream for durability. delivery_events is the source of truth; the stream is a transport.

Consumer groups need care. If a gateway crashes  mid-processing, its pending entries sit in the PEL (pending entries list) until claimed. You need a XAUTOCLAIM/XCLAIM loop to reclaim them, or messages stall. This is real operational work.

One stream per gateway (stream:gateway:{id}): relay routes by session:{device_id} → gateway_id, writes only to the relevant stream. Efficient, but you need to manage stream lifecycle as gateways come and go.or shard by gateway ID. . Noted #TODO
    When gateway go(stops): Simplest cleanup: TTL the stream (EXPIRE) and let it vanish after the gateway's been gone for a while   


Design so the transport is swappable.



## Note on WS routing

>Thinking about making subsequent connections after one, if existing, go to the
>same WS server — so we can save routing to multiple WS servers.
>
>But again, this will add work, bring some cons, LB is not easy, also, such
>users are rare, so, let it be..
>
>Each device just connects wherever the LB drops it. No sticky routing, no
>hunting for where the user's other session already is. Registry keeps track
>of user's connections, delivery worker reads it and pushes to whichever
>nodes hold them.
>
>If the former idea comes relevant in future, it could be implemented, seems
>there won't be much design alterations required then..