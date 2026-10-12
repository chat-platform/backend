-- Local fixtures for Phase 0–3 (no Core API yet).
-- Alice (device-a) <-> Bob (device-b) in one DIRECT chat.

INSERT INTO users (user_id, phone, username, display_name)
VALUES
    ('11111111-1111-4111-8111-111111111111', '+10000000001', 'alice', 'Alice'),
    ('22222222-2222-4222-8222-222222222222', '+10000000002', 'bob', 'Bob');

INSERT INTO device (device_id, user_id, platform, is_primary, device_name)
VALUES
    ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', '11111111-1111-4111-8111-111111111111', 'WEB', true, 'Alice Web'),
    ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', '22222222-2222-4222-8222-222222222222', 'WEB', true, 'Bob Web');

INSERT INTO chat (chat_id, type, name, created_by)
VALUES
    ('cccccccc-cccc-4ccc-8ccc-cccccccccccc', 'DIRECT', NULL, '11111111-1111-4111-8111-111111111111');

INSERT INTO chat_membership (chat_id, user_id, role)
VALUES
    ('cccccccc-cccc-4ccc-8ccc-cccccccccccc', '11111111-1111-4111-8111-111111111111', 'member'),
    ('cccccccc-cccc-4ccc-8ccc-cccccccccccc', '22222222-2222-4222-8222-222222222222', 'member');
