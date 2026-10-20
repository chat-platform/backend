ALTER TABLE group_outbox DROP COLUMN attempt_count;
ALTER TABLE direct_outbox DROP COLUMN attempt_count;
DROP INDEX direct_outbox_message_idempotency_idx;
ALTER TABLE direct_outbox
    ADD CONSTRAINT direct_outbox_sender_id_client_msg_id_device_id_key
    UNIQUE (sender_id, client_msg_id, device_id);
