package httpadapter

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (s *Server) setPushToken(c echo.Context) error {
	var body struct {
		Token string `json:"token" validate:"required,max=4096"`
	}
	if err := decode(c, &body); err != nil {
		return s.writeInvalidRequest(c, err)
	}
	if err := s.service.SetPushToken(c.Request().Context(), claimsFrom(c.Request()).DeviceID, body.Token); err != nil {
		return s.writeServiceError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}

func (s *Server) clearPushToken(c echo.Context) error {
	if err := s.service.ClearPushToken(c.Request().Context(), claimsFrom(c.Request()).DeviceID); err != nil {
		return s.writeServiceError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}

func (s *Server) profile(c echo.Context) error {
	user, err := s.service.Profile(c.Request().Context(), claimsFrom(c.Request()).UserID)
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, map[string]string{"userId": user.ID, "phone": user.Phone, "displayName": user.DisplayName})
}

func (s *Server) setName(c echo.Context) error {
	var body struct {
		DisplayName string `json:"displayName" validate:"required,min=1,max=80"`
	}
	if err := decode(c, &body); err != nil {
		return s.writeInvalidRequest(c, err)
	}
	if err := s.service.SetDisplayName(c.Request().Context(), claimsFrom(c.Request()).UserID, body.DisplayName); err != nil {
		return s.writeServiceError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}
