ALTER TABLE device ADD COLUMN IF NOT EXISTS protocol_device_id SMALLINT;
WITH ranked AS (
    SELECT device_id, row_number() OVER (PARTITION BY user_id ORDER BY created_at, device_id) AS ordinal
    FROM device WHERE protocol_device_id IS NULL
)
UPDATE device SET protocol_device_id = ranked.ordinal
FROM ranked WHERE ranked.device_id = device.device_id AND device.protocol_device_id IS NULL;
ALTER TABLE device ALTER COLUMN protocol_device_id SET NOT NULL;
DO $$ BEGIN
    ALTER TABLE device ADD CONSTRAINT device_protocol_device_id_range CHECK (protocol_device_id BETWEEN 1 AND 255);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS device_user_protocol_device_id_idx ON device (user_id, protocol_device_id) WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS user_device_state (
    user_id UUID PRIMARY KEY REFERENCES users (user_id) ON DELETE CASCADE,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS device_e2ee_key (
    device_id UUID PRIMARY KEY REFERENCES device (device_id) ON DELETE CASCADE,
    registration_id INTEGER NOT NULL CHECK (registration_id BETWEEN 1 AND 16380),
    identity_key BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS device_signed_prekey (
    device_id UUID NOT NULL REFERENCES device (device_id) ON DELETE CASCADE,
    prekey_id INTEGER NOT NULL CHECK (prekey_id BETWEEN 1 AND 2147483647),
    public_key BYTEA NOT NULL,
    signature BYTEA NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_current BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (device_id, prekey_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS device_signed_prekey_current_idx ON device_signed_prekey (device_id) WHERE is_current;

CREATE TABLE IF NOT EXISTS device_kyber_prekey (
    device_id UUID NOT NULL REFERENCES device (device_id) ON DELETE CASCADE,
    prekey_id INTEGER NOT NULL CHECK (prekey_id BETWEEN 1 AND 2147483647),
    public_key BYTEA NOT NULL,
    signature BYTEA NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_current BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (device_id, prekey_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS device_kyber_prekey_current_idx ON device_kyber_prekey (device_id) WHERE is_current;

CREATE TABLE IF NOT EXISTS device_one_time_prekey (
    device_id UUID NOT NULL REFERENCES device (device_id) ON DELETE CASCADE,
    prekey_id INTEGER NOT NULL CHECK (prekey_id BETWEEN 1 AND 2147483647),
    public_key BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (device_id, prekey_id)
);
CREATE INDEX IF NOT EXISTS device_one_time_prekey_inventory_idx ON device_one_time_prekey (device_id, created_at, prekey_id);
