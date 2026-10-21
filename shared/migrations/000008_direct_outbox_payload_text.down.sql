ALTER TABLE direct_outbox
    ALTER COLUMN payload TYPE JSONB USING payload::jsonb;
