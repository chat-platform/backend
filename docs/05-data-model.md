chat_membership (includes group and personal)
----------------
chat_id → chat.chat_id
user_id → user.user_id
role
joined_at
member_tag

chat
----
chat_id
type //DIRECT,GROUP,etc
name
profile_pic
created_by
created_at
updated_at
deleted_at

chat_msg
--------
message_id
chat_id → chat.chat_id
sender_id → user.user_id
client_msg_id //for Client->Server idempotency
content
message_type //eg: TEXT,MEDIA etc
created_at

msg_seen_status
---------------
message_id     → chat_msg.message_id
user_id        → user.user_id  -- the recipient
delivered_at
read_at

msg_seen_status = per-user read state (has this user seen this message, across any of their devices)
-- Granularity: one row per (message, recipient user).
-- Aggregates read state across all the user's devices.
-- Different question than outbox: outbox asks "did device D get it?",
-- this asks "has the user seen it anywhere?"

chat_msg_media
--------------
message_id → chat_msg.message_id
media_id → media.media_id
position // optional, useful if multiple media per message

users
-----
user_id
phone
username
display_name
profile_pic_media_id → profile_pic_media.ppic_id
about (usually used for statuses like 'At work', 'Away, leave a msg',etc)
created_at
updated_at

profile_pic_media
-----------------
ppic_id
thumbnail_storage_key
full_storage_key
status (pending,ready,failed,etc. Only upon 'ready' shall the profile pic be reflected -->)
mime_type
size
created_at
deleted_at

media
-----
media_id
file_name
caption //text along with the media
thumbnail_storage_key
full_storage_key
mime_type
size
created_at
deleted_at
...

phone_history (or as event log?)
-------------
user_id → user.user_id
old_phone
new_phone
changed_at

user_blocks
-------------
blocker_id → user.user_id
blocked_id → user.user_id
created_at

notes: No seperate delivery record for each device

device
------
device_id
user_id → user.user_id
platform          // ANDROID / IOS / WEB
device_name
created_at
last_seen_at
revoked_at

session
-------
session_id
device_id → device.device_id
created_at
refresh_last_used_at
refresh_expires_at
revoked_at

note: no refresh token/its hash here, 
    instead in refresh token, there shall be sessionId. 
    Each refresh token be for each session.
    If required immediate session revoke, lets introduce jti here later

outbox
------
id             PK      -- global monotonic; the device cursor
message_id             -- FK → chat_msg.message_id
chat_id                -- denormalized, for routing/cleanup
recipient_id           -- FK → user.user_id (the target user)
device_id              -- FK → device.device_id (the target device)
sender_id              -- FK → user.user_id (who caused it)
event_type             -- NEW_MESSAGE | EDIT | DELETE | REACTION | ...
payload                -- event-specific delta (NULL for NEW_MESSAGE)
created_at             
dispatched_at          -- when handed to a ws gateway/or to the msg broker
delivered_at           -- when device acked RECEIVED
read_at                -- when device acked SEEN

TODO: do we need read_at here as there is already msg_seen_status table?

outbox = per-device delivery/transport record (how do I get this event to this specific device)
-- Granularity: one outbox row per (message, recipient device).
-- This row *is* the per-device delivery record — no separate table.

-----------------------------------
user
 │
 └── 1:N device
          │
          └── 1:N session (incl old sessions)

--------------------------------------

users
  │
  │ 1:N
  ▼
chat_membership
  │
  │ N:1
  ▼
chat
  │
  │ 1:N
  ▼
messages

## Important Access Patterns

### Users
- Find user by phone
- Get user's devices

### Chats
- Get chats for user
- Get members of chat

### Messages
- Insert message
- Get messages for chat ordered by time
- Get messages after cursor
- Find message by client_msg_id

### Outbox
- Get pending events for device
- Get events after cursor
- Mark event delivered
- Mark event read

# Other
## Redis - WS registruy
    key: ws:conn:{device_id}
    value: { ws_node, conn_id }
    TTL:   refreshed on heartbeat (e.g. 90s, 3× heartbeat cadence)
    notes:
        - Presence in this key = device has a live socket.
        - Explicit DEL on clean disconnect.
        - Unclean disconnect: TTL expires the key; no reaper needed.
        - Device-to-user mapping is in Postgres (device table), not here.