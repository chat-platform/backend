package httpadapter

import (
	"net/http"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/labstack/echo/v4"
)

func (s *Server) registerDeviceKeys(c echo.Context) error {
	var body app.DeviceKeysRegistration
	if err := decode(c, &body); err != nil {
		return s.writeInvalidRequest(c, err)
	}
	version, err := s.service.RegisterDeviceKeys(c.Request().Context(), claimsFrom(c.Request()), body)
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, map[string]int64{"deviceListVersion": version})
}

func (s *Server) uploadOneTimePreKeys(c echo.Context) error {
	var body struct {
		Keys []app.PublicPreKey `json:"keys" validate:"required,min=1,max=100,dive"`
	}
	if err := decode(c, &body); err != nil {
		return s.writeInvalidRequest(c, err)
	}
	if err := s.service.UploadOneTimePreKeys(c.Request().Context(), claimsFrom(c.Request()), body.Keys); err != nil {
		return s.writeServiceError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}

func (s *Server) oneTimePreKeyCount(c echo.Context) error {
	count, err := s.service.CountOneTimePreKeys(c.Request().Context(), claimsFrom(c.Request()))
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, map[string]int{"count": count})
}

func (s *Server) directDevices(c echo.Context) error {
	list, err := s.service.DirectDeviceList(c.Request().Context(), claimsFrom(c.Request()), c.Param("id"))
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, list)
}

func (s *Server) preKeyBundle(c echo.Context) error {
	bundle, err := s.service.PreKeyBundle(c.Request().Context(), claimsFrom(c.Request()), c.Param("id"), c.Param("deviceId"))
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, bundle)
}
