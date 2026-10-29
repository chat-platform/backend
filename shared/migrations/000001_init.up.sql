-- Baseline schema for a new database. Keep this file aligned with the current
-- application schema; existing databases must not apply it again.

CREATE TYPE device_platform AS ENUM ('ANDROID', 'IOS', 'WEB');
CREATE TYPE chat_membership_role AS ENUM ('admin', 'member');

CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    phone TEXT NOT NULL UNIQUE,
    username TEXT,
    display_name TEXT,
    profile_pic_media_id UUID,
    about TEXT,
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE user_blocks (
    blocker_id UUID NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    blocked_id UUID NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (blocker_id, blocked_id),
    CHECK (blocker_id <> blocked_id)
);

CREATE INDEX user_blocks_blocked_id_idx ON user_blocks (blocked_id);

CREATE TABLE device (
    device_id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users (user_id),
    platform device_platform NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT true,
    device_name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    push_token TEXT
);

CREATE INDEX device_user_id_idx ON device (user_id);

CREATE TABLE session (
    session_id UUID PRIMARY KEY,
    device_id UUID NOT NULL REFERENCES device (device_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);

CREATE INDEX session_device_id_idx ON session (device_id, expires_at DESC);
CREATE INDEX session_active_idx ON session (session_id) WHERE revoked_at IS NULL;

CREATE TABLE chat (
    chat_id UUID PRIMARY KEY,
    type TEXT NOT NULL CHECK (type IN ('DIRECT', 'GROUP')),
    name TEXT,
    profile_pic TEXT,
    created_by UUID REFERENCES users (user_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE chat_membership (
    chat_id UUID NOT NULL REFERENCES chat (chat_id),
    user_id UUID NOT NULL REFERENCES users (user_id),
    role chat_membership_role NOT NULL DEFAULT 'member',
    joined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    member_tag TEXT,
    PRIMARY KEY (chat_id, user_id)
);

CREATE INDEX chat_membership_user_id_idx ON chat_membership (user_id);

CREATE TABLE direct_outbox (
    id BIGSERIAL PRIMARY KEY,
    sender_id UUID NOT NULL REFERENCES users (user_id),
    client_msg_id UUID NOT NULL,
    recipient_id UUID NOT NULL REFERENCES users (user_id),
    device_id UUID NOT NULL REFERENCES device (device_id),
    chat_id UUID REFERENCES chat (chat_id),
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    routed_to TEXT CHECK (routed_to IN ('WS', 'NOTIF')),-- Consider ENUM later for more compact storage.
    dispatched_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    claim_expiry TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX direct_outbox_device_id_idx ON direct_outbox (device_id, id);
CREATE INDEX direct_outbox_undispatched_idx ON direct_outbox (id) WHERE dispatched_at IS NULL;
CREATE INDEX direct_outbox_claim_expiry_idx
    ON direct_outbox (claim_expiry, id)
    WHERE dispatched_at IS NULL;
CREATE INDEX direct_outbox_expiry_cleanup_idx ON direct_outbox (created_at);
CREATE UNIQUE INDEX direct_outbox_message_idempotency_idx
    ON direct_outbox (sender_id, client_msg_id, device_id)
    WHERE event_type = 'NEW_MESSAGE';
CREATE INDEX direct_outbox_chat_id_idx ON direct_outbox (chat_id) WHERE chat_id IS NOT NULL;

CREATE TABLE group_event (
    event_id BIGSERIAL PRIMARY KEY,
    chat_id UUID NOT NULL REFERENCES chat (chat_id),
    sender_id UUID NOT NULL REFERENCES users (user_id),
    client_msg_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,-- Transport-only payload; no inner querying, so not JSONB as of now. avoiding JSONB parsing overhead
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (sender_id, client_msg_id)
);

CREATE INDEX group_event_chat_id_event_id_idx ON group_event (chat_id, event_id);

CREATE TABLE group_outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id BIGINT NOT NULL REFERENCES group_event (event_id) ON DELETE CASCADE,
    recipient_id UUID NOT NULL REFERENCES users (user_id),
    device_id UUID NOT NULL REFERENCES device (device_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    routed_to TEXT CHECK (routed_to IN ('WS', 'NOTIF')),
    dispatched_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    claim_expiry TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE (event_id, device_id)
);

CREATE INDEX group_outbox_device_id_idx ON group_outbox (device_id, id);
CREATE INDEX group_outbox_undispatched_idx ON group_outbox (id) WHERE dispatched_at IS NULL;

CREATE INDEX direct_outbox_ws_retry_pending_snapshot_idx
    ON direct_outbox (dispatched_at, id)
    WHERE routed_to = 'WS' AND delivered_at IS NULL AND attempt_count < 10;
CREATE INDEX direct_outbox_ws_retry_exhausted_snapshot_idx
    ON direct_outbox (id)
    WHERE routed_to = 'WS' AND delivered_at IS NULL AND attempt_count >= 10;
CREATE INDEX direct_outbox_ws_overdue_message_snapshot_idx
    ON direct_outbox (created_at)
    WHERE event_type = 'NEW_MESSAGE' AND routed_to = 'WS' AND delivered_at IS NULL;

CREATE INDEX group_outbox_ws_retry_pending_snapshot_idx
    ON group_outbox (dispatched_at, id)
    WHERE routed_to = 'WS' AND delivered_at IS NULL AND attempt_count < 10;
CREATE INDEX group_outbox_ws_retry_exhausted_snapshot_idx
    ON group_outbox (id)
    WHERE routed_to = 'WS' AND delivered_at IS NULL AND attempt_count >= 10;
CREATE INDEX group_outbox_ws_overdue_snapshot_idx
    ON group_outbox (created_at, event_id)
    WHERE routed_to = 'WS' AND delivered_at IS NULL;
