DROP INDEX IF EXISTS direct_outbox_reaction_idempotency_idx;

ALTER TABLE group_event
    DROP CONSTRAINT IF EXISTS group_event_sender_client_event_key;

ALTER TABLE group_event
    ADD CONSTRAINT group_event_sender_id_client_msg_id_key
    UNIQUE (sender_id, client_msg_id);
