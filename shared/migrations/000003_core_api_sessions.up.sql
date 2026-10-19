CREATE TABLE session (
    session_id UUID PRIMARY KEY,
    device_id UUID NOT NULL REFERENCES device (device_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);

CREATE INDEX session_device_id_idx ON session (device_id, expires_at DESC);
CREATE INDEX session_active_idx ON session (session_id) WHERE revoked_at IS NULL;
