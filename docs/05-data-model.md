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

device
------
device_id
user_id → user.user_id
platform          // ANDROID / IOS / WEB
is_primary        -- true for the phone, false for companions like whatsapp-web
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

direct_outbox
-------------
id             PK      -- per-device cursor
sender_id              -- FK → user.user_id
client_msg_id   TEXT -- 
recipient_id           -- FK → user.user_id
device_id              -- FK → device.device_id
event_type             -- NEW_MESSAGE | EDIT | DELETE | REACTION | ...
payload                -- the wire payload (content included)
created_at
routed_to              -- ENUM: WS, NOTIF
dispatched_at
delivered_at

-- UNIQUE (sender_id, client_msg_id, device_id)
-- Idempotency: on retry, the insert conflicts, and Message Service
-- returns the same ack without re-inserting.
-- Granularity: one row per (message, recipient device) in a DIRECT chat.
-- Carries the full payload; no join needed at dispatch.
-- Events addressed to a single user's devices:
--   - NEW_MESSAGE / EDIT / DELETE / REACTION in a DIRECT chat
--   - RECEIPT for any message (direct or group) — goes to the sender only

group_outbox
------------
id             PK      -- per-device cursor
event_id       FK → group_event.event_id
recipient_id           -- FK → user.user_id
device_id              -- FK → device.device_id
created_at
routed_to
dispatched_at
delivered_at

-- UNIQUE (sender_id, client_msg_id)
-- Idempotency: same as direct_outbox, one row per group message.
-- Granularity: one row per (group event, recipient device).
-- No payload here; join group_event on event_id at dispatch.
-- Events addressed to all members of a GROUP chat:
--   - NEW_MESSAGE / EDIT / DELETE / REACTION in a GROUP chat
--   (Receipts do NOT go here; they go to direct_outbox.)

group_event
-----------
event_id       PK
chat_id        FK → chat.chat_id
sender_id      FK → user.user_id
client_msg_id   TEXT
event_type
payload        -- the wire payload (content included)
created_at

-- One row per group event. Shared by all recipient devices.
-- This is where the payload lives, so it isn't duplicated per device.

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
  ├── 1:N ──> direct_outbox   (for DIRECT chats)
  └── 1:N ──> group_event     (for GROUP chats)

## Important Access Patterns

### Users
- Find user by phone
- Get user's devices

### Chats
- Get chats for user
- Get members of chat

### Outboxes
- Get pending events for device (direct_outbox, group_outbox)
- Get events after cursor
- Mark event dispatched
- Mark event delivered

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