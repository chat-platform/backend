-- Upgrade an initialized database to UUID-backed client message IDs. This is
-- transactional so invalid historical text IDs leave the old schema intact.
BEGIN;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'direct_outbox'::regclass
          AND contype = 'u'
          AND pg_get_constraintdef(oid) LIKE '%originating_device_id%'
    LOOP
        EXECUTE format('ALTER TABLE direct_outbox DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END $$;

ALTER TABLE direct_outbox DROP COLUMN IF EXISTS originating_device_id;
ALTER TABLE direct_outbox
    ALTER COLUMN client_msg_id TYPE UUID USING client_msg_id::uuid;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'direct_outbox'::regclass
          AND contype = 'u'
          AND pg_get_constraintdef(oid) = 'UNIQUE (sender_id, client_msg_id, device_id)'
    ) THEN
        ALTER TABLE direct_outbox
            ADD CONSTRAINT direct_outbox_sender_client_device_key
            UNIQUE (sender_id, client_msg_id, device_id);
    END IF;
END $$;

COMMIT;
