-- Group chats need a display name; direct chats may keep name unset.
UPDATE chat
SET name = 'Group'
WHERE type = 'GROUP' AND (name IS NULL OR length(btrim(name)) = 0);

ALTER TABLE chat
    ADD CONSTRAINT chat_group_name_required
    CHECK (type <> 'GROUP' OR (name IS NOT NULL AND length(btrim(name)) > 0));
