-- Phase 0 schema: tables Message Service and Relay need for 1:1 send.
-- Group tables, sessions, blocks, and media are deferred.

CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    phone TEXT NOT NULL UNIQUE,
    username TEXT,
    display_name TEXT,
    profile_pic_media_id UUID,
    about TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE device (
    device_id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users (user_id),
    platform TEXT NOT NULL CHECK (platform IN ('ANDROID', 'IOS', 'WEB')),-- Consider ENUM later for more compact storage.
    is_primary BOOLEAN NOT NULL DEFAULT true,
    device_name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ
);

CREATE INDEX device_user_id_idx ON device (user_id);

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
    role TEXT NOT NULL DEFAULT 'member',-- Consider ENUM later for more compact storage.
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
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL, -- Transport-only payload; no inner querying, so not JSONB as of now. avoiding JSONB parsing overhead
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    routed_to TEXT CHECK (routed_to IN ('WS', 'NOTIF')),-- Consider ENUM later for more compact storage.
    dispatched_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    UNIQUE (sender_id, client_msg_id, device_id)
);

CREATE INDEX direct_outbox_device_id_idx ON direct_outbox (device_id, id);
CREATE INDEX direct_outbox_undispatched_idx ON direct_outbox (id) WHERE dispatched_at IS NULL;
