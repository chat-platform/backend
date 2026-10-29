package httpadapter

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
)

func bearerToken(r *http.Request) (string, bool) {
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	return parts[1], true
}

func (s *Server) requestOTP(c echo.Context) error {
	var body struct {
		Phone string `json:"phone" validate:"e164"`
	}
	if err := decode(c, &body); err != nil {
		return s.writeInvalidRequest(c, err)
	}
	if err := s.service.RequestOTP(c.Request().Context(), body.Phone); err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusAccepted, map[string]string{"status": "otp_requested"})
}

func (s *Server) verifyOTP(c echo.Context) error {
	var body struct {
		Phone      string `json:"phone" validate:"e164"`
		Code       string `json:"code" validate:"required"`
		Platform   string `json:"platform" validate:"required"`
		DeviceName string `json:"deviceName"`
		DeviceID   string `json:"deviceId"`
	}
	if err := decode(c, &body); err != nil {
		return s.writeInvalidRequest(c, err)
	}
	result, err := s.service.VerifyOTP(c.Request().Context(), body.Phone, body.Code, body.Platform, body.DeviceName, body.DeviceID)
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, result)
}

func (s *Server) validateSession(c echo.Context) error {
	token, ok := bearerToken(c.Request())
	if !ok {
		return s.writeExpectedError(c, "session validation rejected", errors.New("missing or malformed bearer authorization"), http.StatusUnauthorized, "unauthorized", "valid bearer token required")
	}
	claims, err := s.service.Authenticate(c.Request().Context(), token)
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, claims)
}

func (s *Server) logout(c echo.Context) error {
	if err := s.service.Logout(c.Request().Context(), claimsFrom(c.Request())); err != nil {
		return s.writeServiceError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}
