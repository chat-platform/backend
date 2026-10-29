-- Drop the complete baseline schema in dependency order.
DROP TABLE IF EXISTS group_outbox;
DROP TABLE IF EXISTS group_event;
DROP TABLE IF EXISTS session;
DROP TABLE IF EXISTS direct_outbox;
DROP TABLE IF EXISTS chat_membership;
DROP TABLE IF EXISTS chat;
DROP TABLE IF EXISTS device;
DROP TABLE IF EXISTS user_blocks;
DROP TABLE IF EXISTS users;
