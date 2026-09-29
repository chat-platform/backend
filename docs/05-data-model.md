chat_membership (includes group and personal)
----------------
chat_id
user_id
role
joined_at
member_tag

chat
----
chat_id
type
name
profile_pic
created_by
created_at
updated_at
deleted_at

chat_msg
--------
message_id
chat_id
sender_id
client_msg_id //for Client->Server idempotency
content
created_at
media_id <!-- optional -->

users
-----
user_id
phone
username
display_name
profile_pic_media_id <!---better to keep it as seperate table if any metadata might become relevant in future-->
about (usually used for statuses like 'At work', 'Away, leave a msg',etc)
created_at
updated_at

profile_pic_media
-----------------
media_id
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
thumbnail_storage_key
full_storage_key
mime_type
size
created_at
deleted_at
...

phone_history (or as event log?)
-------------
user_id
old_phone
new_phone
changed_at

user_blocks
-------------
blocker_id
blocked_id
created_at

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