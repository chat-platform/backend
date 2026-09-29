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
created_at
updated_at
name
profile_pic
created_by

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
created_at
updated_at

profile_pic_media
-----------------
media_id
storage_key
mime_type
size

media
-----
media_id
file_name
storage_key
mime_type
size
...

phone_history (or as event log?)
-------------
user_id
old_phone
new_phone
changed_at

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