CREATE INDEX direct_outbox_delivered_cleanup_idx
    ON direct_outbox (delivered_at, id)
    WHERE delivered_at IS NOT NULL;

CREATE INDEX direct_outbox_expiry_cleanup_idx
    ON direct_outbox (created_at, id)
    WHERE delivered_at IS NULL;
