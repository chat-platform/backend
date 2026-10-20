CREATE INDEX direct_outbox_expiry_cleanup_idx
    ON direct_outbox (created_at);
