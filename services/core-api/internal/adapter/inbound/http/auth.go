package httpadapter

import (
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
		Phone string `json:"phone"`
	}
	if !decode(c, &body) {
		return nil
	}
	if err := s.service.RequestOTP(c.Request().Context(), body.Phone); err != nil {
		return writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusAccepted, map[string]string{"status": "otp_requested"})
}

func (s *Server) verifyOTP(c echo.Context) error {
	var body struct {
		Phone      string `json:"phone"`
		Code       string `json:"code"`
		Platform   string `json:"platform"`
		DeviceName string `json:"deviceName"`
		DeviceID   string `json:"deviceId"`
	}
	if !decode(c, &body) {
		return nil
	}
	result, err := s.service.VerifyOTP(c.Request().Context(), body.Phone, body.Code, body.Platform, body.DeviceName, body.DeviceID)
	if err != nil {
		return writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, result)
}

func (s *Server) validateSession(c echo.Context) error {
	token, ok := bearerToken(c.Request())
	if !ok {
		return writeError(c, http.StatusUnauthorized, "unauthorized", "valid bearer token required")
	}
	claims, err := s.service.Authenticate(c.Request().Context(), token)
	if err != nil {
		return writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, claims)
}

func (s *Server) logout(c echo.Context) error {
	if err := s.service.Logout(c.Request().Context(), claimsFrom(c.Request())); err != nil {
		return writeServiceError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}
