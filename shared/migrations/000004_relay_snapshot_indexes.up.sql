CREATE INDEX direct_outbox_ws_retry_pending_snapshot_idx
    ON direct_outbox (dispatched_at, id)
    WHERE routed_to = 'WS' AND delivered_at IS NULL AND attempt_count < 10;
CREATE INDEX direct_outbox_ws_retry_exhausted_snapshot_idx
    ON direct_outbox (id)
    WHERE routed_to = 'WS' AND delivered_at IS NULL AND attempt_count >= 10;
CREATE INDEX direct_outbox_ws_overdue_message_snapshot_idx
    ON direct_outbox (created_at)
    WHERE event_type = 'NEW_MESSAGE' AND routed_to = 'WS' AND delivered_at IS NULL;

CREATE INDEX group_outbox_ws_retry_pending_snapshot_idx
    ON group_outbox (dispatched_at, id)
    WHERE routed_to = 'WS' AND delivered_at IS NULL AND attempt_count < 10;
CREATE INDEX group_outbox_ws_retry_exhausted_snapshot_idx
    ON group_outbox (id)
    WHERE routed_to = 'WS' AND delivered_at IS NULL AND attempt_count >= 10;
CREATE INDEX group_outbox_ws_overdue_snapshot_idx
    ON group_outbox (created_at, event_id)
    WHERE routed_to = 'WS' AND delivered_at IS NULL;
