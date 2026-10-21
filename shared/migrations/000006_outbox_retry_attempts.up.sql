ALTER TABLE direct_outbox
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE group_outbox
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;

UPDATE direct_outbox SET attempt_count=1 WHERE dispatched_at IS NOT NULL AND routed_to='WS';
UPDATE group_outbox SET attempt_count=1 WHERE dispatched_at IS NOT NULL AND routed_to='WS';

ALTER TABLE direct_outbox
    DROP CONSTRAINT direct_outbox_sender_id_client_msg_id_device_id_key;

CREATE UNIQUE INDEX direct_outbox_message_idempotency_idx
    ON direct_outbox (sender_id, client_msg_id, device_id)
    WHERE event_type = 'NEW_MESSAGE';
