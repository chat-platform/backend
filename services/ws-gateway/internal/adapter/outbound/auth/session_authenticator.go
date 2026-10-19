package authadapter

import (
	"context"
	"errors"
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
		return domain.Identity{}, domain.ErrInvalidIdentity
	}
	status, err := a.status.SessionStatus(ctx, claims.SessionID)
	if err != nil {
		return domain.Identity{}, err
	}
	switch status {
	case sessiontoken.StatusActive:
	case sessiontoken.StatusRevoked:
		return domain.Identity{}, domain.ErrInvalidIdentity
	case sessiontoken.StatusMissing:
		if a.validator == nil {
			return domain.Identity{}, errors.New("session validation fallback is unavailable")
		}
		validated, err := a.validator.ValidateSession(ctx, token)
		if err != nil {
			return domain.Identity{}, err
		}
		if validated != claims {
			return domain.Identity{}, domain.ErrInvalidIdentity
		}
	default:
		return domain.Identity{}, domain.ErrInvalidIdentity
	}
	return domain.Identity{UserID: claims.UserID, DeviceID: claims.DeviceID, SessionID: claims.SessionID}, nil
}
func (a *SessionAuthenticator) IsSessionActive(ctx context.Context, token string) (bool, error) {
	_, err := a.Authenticate(ctx, token)
	if errors.Is(err, domain.ErrInvalidIdentity) {
		return false, nil
	}
	return err == nil, err
}

var _ port.SessionAuthenticator = (*SessionAuthenticator)(nil)
