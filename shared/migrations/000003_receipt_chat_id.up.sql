ALTER TABLE direct_outbox
    ADD COLUMN chat_id UUID REFERENCES chat (chat_id);

CREATE INDEX direct_outbox_chat_id_idx ON direct_outbox (chat_id) WHERE chat_id IS NOT NULL;
