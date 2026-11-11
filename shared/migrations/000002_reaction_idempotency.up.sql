-- Group events use event-type-scoped client operation IDs. The application
-- already targets this key for idempotent inserts.
ALTER TABLE group_event
    DROP CONSTRAINT IF EXISTS group_event_sender_id_client_msg_id_key;

ALTER TABLE group_event
    ADD CONSTRAINT group_event_sender_client_event_key
    UNIQUE (sender_id, client_msg_id, event_type);

CREATE UNIQUE INDEX direct_outbox_reaction_idempotency_idx
    ON direct_outbox (sender_id, client_msg_id, device_id)
    WHERE event_type = 'REACTION';
