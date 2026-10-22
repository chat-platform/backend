package authadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/services/ws-gateway/internal/port"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

type StatusReader interface {
	SessionStatus(context.Context, string) (string, error)
}
type SessionValidator interface {
	ValidateSession(context.Context, string) (sessiontoken.Claims, error)
}
type SessionAuthenticator struct {
	secret    []byte
	status    StatusReader
	validator SessionValidator
}

func New(secret []byte, status StatusReader, validator SessionValidator) *SessionAuthenticator {
	return &SessionAuthenticator{secret: secret, status: status, validator: validator}
}
func (a *SessionAuthenticator) Authenticate(ctx context.Context, token string) (domain.Identity, error) {
	claims, err := sessiontoken.Verify(token, a.secret, time.Now())
	if err != nil {
		return domain.Identity{}, fmt.Errorf("%w: verify session token: %w", domain.ErrInvalidIdentity, err)
	}
	status, err := a.status.SessionStatus(ctx, claims.SessionID)
	if err != nil {
		return domain.Identity{}, fmt.Errorf("read cached status for session %s: %w", claims.SessionID, err)
	}
	switch status {
	case sessiontoken.StatusActive:
	case sessiontoken.StatusRevoked:
		return domain.Identity{}, fmt.Errorf("%w: session %s is revoked", domain.ErrInvalidIdentity, claims.SessionID)
	case sessiontoken.StatusMissing:
		if a.validator == nil {
			return domain.Identity{}, errors.New("validate session cache miss: fallback validator is unavailable")
		}
		validated, err := a.validator.ValidateSession(ctx, token)
		if err != nil {
			return domain.Identity{}, fmt.Errorf("validate session cache miss with Core API: %w", err)
		}
		if validated != claims {
			return domain.Identity{}, fmt.Errorf("%w: Core API claims do not match signed session %s", domain.ErrInvalidIdentity, claims.SessionID)
		}
	default:
		return domain.Identity{}, fmt.Errorf("%w: unknown cached session status %q for session %s", domain.ErrInvalidIdentity, status, claims.SessionID)
	}
	return domain.Identity{UserID: claims.UserID, DeviceID: claims.DeviceID, SessionID: claims.SessionID}, nil
}
func (a *SessionAuthenticator) IsSessionActive(ctx context.Context, token string) (bool, error) {
	_, err := a.Authenticate(ctx, token)
	return err == nil, err
}

var _ port.SessionAuthenticator = (*SessionAuthenticator)(nil)
