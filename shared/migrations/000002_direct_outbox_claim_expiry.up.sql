ALTER TABLE direct_outbox
    ADD COLUMN claim_expiry TIMESTAMPTZ;

CREATE INDEX direct_outbox_claim_expiry_idx
    ON direct_outbox (claim_expiry, id)
    WHERE dispatched_at IS NULL;
