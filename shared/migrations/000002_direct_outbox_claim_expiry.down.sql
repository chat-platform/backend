DROP INDEX IF EXISTS direct_outbox_claim_expiry_idx;

ALTER TABLE direct_outbox
    DROP COLUMN IF EXISTS claim_expiry;
