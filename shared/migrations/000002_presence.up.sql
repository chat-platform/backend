ALTER TABLE users
    ADD COLUMN last_seen_at TIMESTAMPTZ;

CREATE TABLE user_blocks (
    blocker_id UUID NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    blocked_id UUID NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (blocker_id, blocked_id),
    CHECK (blocker_id <> blocked_id)
);

CREATE INDEX user_blocks_blocked_id_idx ON user_blocks (blocked_id);
