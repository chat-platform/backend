DROP INDEX IF EXISTS direct_outbox_chat_id_idx;
ALTER TABLE direct_outbox DROP COLUMN IF EXISTS chat_id;
