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

message_delivery 
----------------
message_id → chat_msg.message_id
user_id → user.user_id
delivered_at
read_at

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