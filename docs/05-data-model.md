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

-----
user_id
phone
username
display_name
profile_pic_media_url
created_at
updated_at

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