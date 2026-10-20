CREATE TABLE group_event (
    event_id BIGSERIAL PRIMARY KEY,
    chat_id UUID NOT NULL REFERENCES chat (chat_id),
    sender_id UUID NOT NULL REFERENCES users (user_id),
    client_msg_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
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
    UNIQUE (event_id, device_id)
);

CREATE INDEX group_outbox_device_id_idx ON group_outbox (device_id, id);
CREATE INDEX group_outbox_undispatched_idx ON group_outbox (id) WHERE dispatched_at IS NULL;
