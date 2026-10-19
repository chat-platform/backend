package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

type Server struct {
	service *app.Service
	mux     *http.ServeMux
}

func New(service *app.Service) *Server {
	s := &Server{service: service, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /v1/auth/otp/request", s.requestOTP)
	s.mux.HandleFunc("POST /v1/auth/otp/verify", s.verifyOTP)
	s.mux.Handle("POST /v1/auth/logout", s.auth(http.HandlerFunc(s.logout)))
	s.mux.Handle("GET /v1/profile/me", s.auth(http.HandlerFunc(s.profile)))
	s.mux.Handle("PUT /v1/profile/name", s.auth(http.HandlerFunc(s.setName)))
	s.mux.Handle("POST /v1/chats/direct", s.auth(http.HandlerFunc(s.directChat)))
	return s
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, code, message string) {
	var body errorBody
	body.Error.Code = code
	body.Error.Message = message
	writeJSON(w, status, body)
}
func decode(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body is invalid")
		return false
	}
	return true
}
func (s *Server) requestOTP(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Phone string `json:"phone"`
	}
	if !decode(w, r, &body) {
		return
	}
	if err := s.service.RequestOTP(r.Context(), body.Phone); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "otp_requested"})
}
func (s *Server) verifyOTP(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Phone      string `json:"phone"`
		Code       string `json:"code"`
		Platform   string `json:"platform"`
		DeviceName string `json:"deviceName"`
		DeviceID   string `json:"deviceId"`
	}
	if !decode(w, r, &body) {
		return
	}
	result, err := s.service.VerifyOTP(r.Context(), body.Phone, body.Code, body.Platform, body.DeviceName, body.DeviceID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type identityKey struct{}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Fields(r.Header.Get("Authorization"))
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			writeError(w, http.StatusUnauthorized, "unauthorized", "valid bearer token required")
			return
		}
		claims, err := s.service.Authenticate(r.Context(), parts[1])
		if err != nil {
			writeServiceError(w, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(contextWithClaims(r, claims)))
	})
}

// Kept as a small context helper to avoid exposing auth claims to handlers through request fields.
func contextWithClaims(r *http.Request, claims sessiontoken.Claims) context.Context {
	return context.WithValue(r.Context(), identityKey{}, claims)
}
func claimsFrom(r *http.Request) sessiontoken.Claims {
	claims, _ := r.Context().Value(identityKey{}).(sessiontoken.Claims)
	return claims
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if err := s.service.Logout(r.Context(), claimsFrom(r)); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (s *Server) profile(w http.ResponseWriter, r *http.Request) {
	user, err := s.service.Profile(r.Context(), claimsFrom(r).UserID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"userId": user.ID, "phone": user.Phone, "displayName": user.DisplayName})
}
func (s *Server) setName(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DisplayName string `json:"displayName"`
	}
	if !decode(w, r, &body) {
		return
	}
	if err := s.service.SetDisplayName(r.Context(), claimsFrom(r).UserID, body.DisplayName); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (s *Server) directChat(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Phone string `json:"phone"`
	}
	if !decode(w, r, &body) {
		return
	}
	chatID, err := s.service.DirectChat(r.Context(), claimsFrom(r).UserID, body.Phone)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"chatId": chatID})
}
func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, app.ErrInvalidRequest):
		writeError(w, http.StatusBadRequest, "invalid_request", "request is invalid")
	case errors.Is(err, app.ErrUnauthorized):
		writeError(w, http.StatusUnauthorized, "unauthorized", "authentication failed")
	case errors.Is(err, app.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "resource was not found")
	case errors.Is(err, app.ErrUnavailable):
		writeError(w, http.StatusServiceUnavailable, "unavailable", "authentication state is unavailable")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "request could not be completed")
	}
}
