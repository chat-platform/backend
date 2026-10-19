-- Adopt databases initialized by the legacy Postgres init script. That script
-- applied migrations without recording their versions, so establish the
-- corresponding golang-migrate version before running pending migrations.
DO $$
DECLARE
    legacy_version BIGINT;
BEGIN
    IF to_regclass('public.schema_migrations') IS NOT NULL
       OR to_regclass('public.users') IS NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'direct_outbox'
          AND column_name = 'claim_expiry'
    ) THEN
        legacy_version := 2;
    ELSE
        legacy_version := 1;
    END IF;

    CREATE TABLE public.schema_migrations (
        version BIGINT NOT NULL PRIMARY KEY,
        dirty BOOLEAN NOT NULL
    );

    INSERT INTO public.schema_migrations (version, dirty)
    VALUES (legacy_version, false);
END;
$$;
