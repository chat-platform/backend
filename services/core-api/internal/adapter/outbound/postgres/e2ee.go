package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/jackc/pgx/v5"
)

func (r *Repository) RegisterDeviceKeys(ctx context.Context, userID, deviceID string, keys app.DeviceKeysRegistration, now time.Time) (int64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin E2EE key registration: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO user_device_state (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING`, userID); err != nil {
		return 0, fmt.Errorf("initialize device-list version: %w", err)
	}
	var version int64
	if err = tx.QueryRow(ctx, `SELECT version FROM user_device_state WHERE user_id=$1 FOR UPDATE`, userID).Scan(&version); err != nil {
		return 0, fmt.Errorf("lock device-list version: %w", err)
	}
	var active bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM device WHERE device_id=$1 AND user_id=$2 AND revoked_at IS NULL)`, deviceID, userID).Scan(&active); err != nil {
		return 0, fmt.Errorf("authorize device key registration: %w", err)
	}
	if !active {
		return 0, app.ErrNotFound
	}
	var oldIdentity []byte
	var oldRegistrationID int32
	err = tx.QueryRow(ctx, `SELECT identity_key, registration_id FROM device_e2ee_key WHERE device_id=$1 FOR UPDATE`, deviceID).Scan(&oldIdentity, &oldRegistrationID)
	newIdentity := err == pgx.ErrNoRows
	if err != nil && !newIdentity {
		return 0, fmt.Errorf("read registered device identity: %w", err)
	}
	if !newIdentity && (string(oldIdentity) != string(keys.IdentityKey) || uint32(oldRegistrationID) != keys.RegistrationID) {
		return 0, app.ErrConflict
	}
	if newIdentity {
		if _, err = tx.Exec(ctx, `INSERT INTO device_e2ee_key (device_id, registration_id, identity_key, created_at) VALUES ($1,$2,$3,$4)`, deviceID, keys.RegistrationID, keys.IdentityKey, now); err != nil {
			return 0, fmt.Errorf("store device identity key: %w", err)
		}
		if _, err = tx.Exec(ctx, `UPDATE user_device_state SET version=version+1, updated_at=$2 WHERE user_id=$1`, userID, now); err != nil {
			return 0, fmt.Errorf("advance device-list version: %w", err)
		}
		version++
	}
	if _, err = tx.Exec(ctx, `UPDATE device_signed_prekey SET is_current=false WHERE device_id=$1 AND is_current`, deviceID); err != nil {
		return 0, fmt.Errorf("retire signed prekey: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO device_signed_prekey (device_id,prekey_id,public_key,signature,published_at,is_current) VALUES ($1,$2,$3,$4,$5,true) ON CONFLICT (device_id,prekey_id) DO UPDATE SET public_key=EXCLUDED.public_key, signature=EXCLUDED.signature, published_at=EXCLUDED.published_at, is_current=true`, deviceID, keys.SignedPreKey.ID, keys.SignedPreKey.PublicKey, keys.SignedPreKey.Signature, now); err != nil {
		return 0, fmt.Errorf("publish signed prekey: %w", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE device_kyber_prekey SET is_current=false WHERE device_id=$1 AND is_current`, deviceID); err != nil {
		return 0, fmt.Errorf("retire Kyber prekey: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO device_kyber_prekey (device_id,prekey_id,public_key,signature,published_at,is_current) VALUES ($1,$2,$3,$4,$5,true) ON CONFLICT (device_id,prekey_id) DO UPDATE SET public_key=EXCLUDED.public_key, signature=EXCLUDED.signature, published_at=EXCLUDED.published_at, is_current=true`, deviceID, keys.KyberPreKey.ID, keys.KyberPreKey.PublicKey, keys.KyberPreKey.Signature, now); err != nil {
		return 0, fmt.Errorf("publish Kyber prekey: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit device key registration: %w", err)
	}
	return version, nil
}

func (r *Repository) UploadOneTimePreKeys(ctx context.Context, userID, deviceID string, keys []app.PublicPreKey) error {
	if len(keys) == 0 {
		return app.ErrInvalidRequest
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin one-time prekey upload: %w", err)
	}
	defer tx.Rollback(ctx)
	var active bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM device WHERE device_id=$1 AND user_id=$2 AND revoked_at IS NULL AND EXISTS (SELECT 1 FROM device_e2ee_key WHERE device_id=$1))`, deviceID, userID).Scan(&active); err != nil {
		return fmt.Errorf("authorize one-time prekey upload: %w", err)
	}
	if !active {
		return app.ErrNotFound
	}
	for _, key := range keys {
		_, err = tx.Exec(ctx, `INSERT INTO device_one_time_prekey (device_id,prekey_id,public_key) VALUES ($1,$2,$3) ON CONFLICT (device_id,prekey_id) DO NOTHING`, deviceID, key.ID, key.PublicKey)
		if err != nil {
			return fmt.Errorf("store one-time prekey: %w", err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit one-time prekey upload: %w", err)
	}
	return nil
}

func (r *Repository) CountOneTimePreKeys(ctx context.Context, userID, deviceID string) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx, `SELECT count(*) FROM device_one_time_prekey p JOIN device d USING (device_id) WHERE p.device_id=$1 AND d.user_id=$2 AND d.revoked_at IS NULL`, deviceID, userID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count one-time prekeys: %w", err)
	}
	var owns bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM device WHERE device_id=$1 AND user_id=$2 AND revoked_at IS NULL)`, deviceID, userID).Scan(&owns); err != nil {
		return 0, fmt.Errorf("authorize one-time prekey count: %w", err)
	}
	if !owns {
		return 0, app.ErrNotFound
	}
	return count, nil
}

func (r *Repository) DirectDeviceList(ctx context.Context, userID, chatID string) (app.DirectDeviceList, error) {
	list := app.DirectDeviceList{Versions: map[string]int64{}, Devices: []app.PublicDevice{}}
	var authorized bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM chat c JOIN chat_membership mine ON mine.chat_id=c.chat_id AND mine.user_id=$2 WHERE c.chat_id=$1 AND c.type='DIRECT' AND c.deleted_at IS NULL AND (SELECT count(*) FROM chat_membership cm WHERE cm.chat_id=c.chat_id)=2)`, chatID, userID).Scan(&authorized)
	if err != nil {
		return list, fmt.Errorf("authorize direct device list: %w", err)
	}
	if !authorized {
		return list, app.ErrNotFound
	}
	rows, err := r.pool.Query(ctx, `SELECT cm.user_id::text, COALESCE(s.version,1) FROM chat_membership cm LEFT JOIN user_device_state s USING(user_id) WHERE cm.chat_id=$1 ORDER BY cm.user_id`, chatID)
	if err != nil {
		return list, fmt.Errorf("read direct device-list versions: %w", err)
	}
	for rows.Next() {
		var uid string
		var v int64
		if err := rows.Scan(&uid, &v); err != nil {
			rows.Close()
			return list, fmt.Errorf("scan device-list version: %w", err)
		}
		list.Versions[uid] = v
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return list, fmt.Errorf("read device-list versions: %w", err)
	}
	rows.Close()
	rows, err = r.pool.Query(ctx, `SELECT d.device_id::text,d.user_id::text,d.protocol_device_id,COALESCE(k.registration_id,0),COALESCE(k.identity_key,''::bytea),k.device_id IS NOT NULL FROM chat_membership cm JOIN device d ON d.user_id=cm.user_id AND d.revoked_at IS NULL LEFT JOIN device_e2ee_key k USING(device_id) WHERE cm.chat_id=$1 ORDER BY d.user_id,d.device_id`, chatID)
	if err != nil {
		return list, fmt.Errorf("read direct encrypted devices: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var d app.PublicDevice
		var signalID, reg int32
		if err := rows.Scan(&d.DeviceID, &d.UserID, &signalID, &reg, &d.IdentityKey, &d.E2EEReady); err != nil {
			return list, fmt.Errorf("scan encrypted device: %w", err)
		}
		d.ProtocolDeviceID = uint32(signalID)
		d.RegistrationID = uint32(reg)
		list.Devices = append(list.Devices, d)
	}
	if err := rows.Err(); err != nil {
		return list, fmt.Errorf("read encrypted devices: %w", err)
	}
	return list, nil
}

func (r *Repository) ConsumePreKeyBundle(ctx context.Context, userID, chatID, deviceID string) (app.PublicPreKeyBundle, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return app.PublicPreKeyBundle{}, fmt.Errorf("begin prekey bundle request: %w", err)
	}
	defer tx.Rollback(ctx)
	var bundle app.PublicPreKeyBundle
	var protocolID, registrationID int32
	err = tx.QueryRow(ctx, `SELECT d.device_id::text,d.user_id::text,d.protocol_device_id,k.registration_id,k.identity_key FROM chat c JOIN chat_membership requester ON requester.chat_id=c.chat_id AND requester.user_id=$3 JOIN chat_membership target ON target.chat_id=c.chat_id JOIN device d ON d.user_id=target.user_id AND d.device_id=$2 AND d.revoked_at IS NULL JOIN device_e2ee_key k USING(device_id) WHERE c.chat_id=$1 AND c.type='DIRECT' AND c.deleted_at IS NULL AND (SELECT count(*) FROM chat_membership cm WHERE cm.chat_id=c.chat_id)=2 FOR SHARE OF d,k`, chatID, deviceID, userID).Scan(&bundle.DeviceID, &bundle.UserID, &protocolID, &registrationID, &bundle.IdentityKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.PublicPreKeyBundle{}, app.ErrNotFound
	}
	if err != nil {
		return app.PublicPreKeyBundle{}, fmt.Errorf("authorize prekey bundle request: %w", err)
	}
	bundle.ProtocolDeviceID = uint32(protocolID)
	bundle.RegistrationID = uint32(registrationID)
	err = tx.QueryRow(ctx, `SELECT prekey_id,public_key,signature FROM device_signed_prekey WHERE device_id=$1 AND is_current`, deviceID).Scan(&bundle.SignedPreKey.ID, &bundle.SignedPreKey.PublicKey, &bundle.SignedPreKey.Signature)
	if err != nil {
		return app.PublicPreKeyBundle{}, fmt.Errorf("read signed prekey: %w", err)
	}
	err = tx.QueryRow(ctx, `SELECT prekey_id,public_key,signature FROM device_kyber_prekey WHERE device_id=$1 AND is_current`, deviceID).Scan(&bundle.KyberPreKey.ID, &bundle.KyberPreKey.PublicKey, &bundle.KyberPreKey.Signature)
	if err != nil {
		return app.PublicPreKeyBundle{}, fmt.Errorf("read Kyber prekey: %w", err)
	}
	var oneTime app.PublicPreKey
	err = tx.QueryRow(ctx, `WITH selected AS (SELECT prekey_id FROM device_one_time_prekey WHERE device_id=$1 ORDER BY created_at,prekey_id FOR UPDATE SKIP LOCKED LIMIT 1) DELETE FROM device_one_time_prekey p USING selected s WHERE p.device_id=$1 AND p.prekey_id=s.prekey_id RETURNING p.prekey_id,p.public_key`, deviceID).Scan(&oneTime.ID, &oneTime.PublicKey)
	if err == nil {
		bundle.OneTimePreKey = &oneTime
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return app.PublicPreKeyBundle{}, fmt.Errorf("allocate one-time prekey: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return app.PublicPreKeyBundle{}, fmt.Errorf("commit prekey bundle allocation: %w", err)
	}
	return bundle, nil
}
