DROP TABLE IF EXISTS device_one_time_prekey;
DROP TABLE IF EXISTS device_kyber_prekey;
DROP TABLE IF EXISTS device_signed_prekey;
DROP TABLE IF EXISTS device_e2ee_key;
DROP TABLE IF EXISTS user_device_state;
DROP INDEX IF EXISTS device_user_protocol_device_id_idx;
ALTER TABLE device DROP CONSTRAINT IF EXISTS device_protocol_device_id_range;
ALTER TABLE device DROP COLUMN IF EXISTS protocol_device_id;
