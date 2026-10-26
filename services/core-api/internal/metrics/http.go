package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	sharedmetrics "github.com/chat-platform/backend/shared/pkg/metrics"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
)

const metricStartKey = "core-api-http-metrics-start"

// HTTPMetrics records bounded-cardinality request counts and durations.
type HTTPMetrics struct {
	requests  *prometheus.CounterVec
	durations *prometheus.HistogramVec
}

func NewHTTP(registry prometheus.Registerer) (*HTTPMetrics, error) {
	m := &HTTPMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "core_api_http_requests_total",
			Help: "Completed Core API HTTP requests by route, method, and status class.",
		}, []string{"operation", "method", "status_class"}),
		durations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "core_api_http_request_duration_seconds",
			Help:    "Core API HTTP request duration from handler entry through response completion.",
			Buckets: sharedmetrics.DefaultBuckets(),
		}, []string{"operation", "method", "status_class"}),
	}
	for _, collector := range []prometheus.Collector{m.requests, m.durations} {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register Core API HTTP metrics: %w", err)
		}
	}
	return m, nil
}

// Install records successful/handled responses in middleware and Echo-mapped
// errors after Echo's configured error handler has written the final status.
func (m *HTTPMetrics) Install(e *echo.Echo) {
	previousErrorHandler := e.HTTPErrorHandler
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		previousErrorHandler(err, c)
		if started, ok := c.Get(metricStartKey).(time.Time); ok {
			m.record(c, started)
			c.Set(metricStartKey, nil)
		}
	}
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			started := time.Now()
			err := next(c)
			if err != nil {
				c.Set(metricStartKey, started)
				return err
			}
			m.record(c, started)
			return nil
		}
	})
}

func (m *HTTPMetrics) record(c echo.Context, started time.Time) {
	operation := c.Path()
	if operation == "" {
		operation = "unmatched"
	}
	method := boundedMethod(c.Request().Method)
	statusClass := boundedStatusClass(c.Response().Status)
	labels := []string{operation, method, statusClass}
	m.requests.WithLabelValues(labels...).Inc()
	m.durations.WithLabelValues(labels...).Observe(time.Since(started).Seconds())
}

func boundedMethod(method string) string {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions:
		return strings.ToUpper(method)
	default:
		return "OTHER"
	}
}

func boundedStatusClass(status int) string {
	switch status / 100 {
	case 2:
		return "2xx"
	case 3:
		return "3xx"
	case 4:
		return "4xx"
	default:
		return "5xx"
	}
}
