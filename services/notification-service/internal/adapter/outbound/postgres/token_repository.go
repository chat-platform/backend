package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TokenRepository struct{ pool *pgxpool.Pool }

func NewTokenRepository(pool *pgxpool.Pool) *TokenRepository { return &TokenRepository{pool: pool} }

func (r *TokenRepository) PushToken(ctx context.Context, deviceID string) (string, string, error) {
	var token, platform string
	err := r.pool.QueryRow(ctx, `SELECT COALESCE(push_token, ''), platform FROM device WHERE device_id=$1 AND revoked_at IS NULL`, deviceID).Scan(&token, &platform)
	if err == pgx.ErrNoRows {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("query device push token: %w", err)
	}
	return token, platform, nil
}

func (r *TokenRepository) ClearPushTokenIfCurrent(ctx context.Context, deviceID, token string) error {
	_, err := r.pool.Exec(ctx, `UPDATE device SET push_token = NULL WHERE device_id = $1 AND push_token = $2`, deviceID, token)
	if err != nil {
		return fmt.Errorf("clear stale device push token: %w", err)
	}
	return nil
}
