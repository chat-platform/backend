-- Payloads are opaque transport JSON. Keep the serialized form as text because
-- the database stores and forwards it without querying its JSON fields.
ALTER TABLE direct_outbox
    ALTER COLUMN payload TYPE TEXT USING payload::text;
