package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
)

func TestHTTPMetricsUseRouteTemplateAndFinalStatusOnce(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewHTTP(registry)
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	metrics.Install(e)
	e.GET("/v1/items/:id", func(c echo.Context) error { return c.NoContent(http.StatusOK) })
	e.GET("/v1/errors", func(echo.Context) error { return echo.NewHTTPError(http.StatusNotFound, "missing") })

	serve := func(method, path string) {
		t.Helper()
		recorder := httptest.NewRecorder()
		e.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	}
	serve(http.MethodGet, "/v1/items/user-123")
	serve(http.MethodGet, "/v1/errors")
	serve(http.MethodGet, "/unknown/one")
	serve(http.MethodGet, "/unknown/two")

	assertMetricCount(t, registry, "core_api_http_requests_total", map[string]string{"operation": "/v1/items/:id", "method": "GET", "status_class": "2xx"}, 1)
	assertMetricCount(t, registry, "core_api_http_request_duration_seconds", map[string]string{"operation": "/v1/items/:id", "method": "GET", "status_class": "2xx"}, 1)
	assertMetricCount(t, registry, "core_api_http_requests_total", map[string]string{"operation": "/v1/errors", "method": "GET", "status_class": "4xx"}, 1)
	assertMetricCount(t, registry, "core_api_http_request_duration_seconds", map[string]string{"operation": "/v1/errors", "method": "GET", "status_class": "4xx"}, 1)
	assertMetricCount(t, registry, "core_api_http_requests_total", map[string]string{"operation": "unmatched", "method": "GET", "status_class": "4xx"}, 2)
	assertMetricCount(t, registry, "core_api_http_request_duration_seconds", map[string]string{"operation": "unmatched", "method": "GET", "status_class": "4xx"}, 2)
}

func TestHTTPMetricsIncludeSyncAndBoundMethodAndStatus(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewHTTP(registry)
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	metrics.Install(e)
	e.HEAD("/v1/sync", func(c echo.Context) error { return c.NoContent(http.StatusNoContent) })
	e.Add("CUSTOM", "/v1/custom", func(c echo.Context) error { return c.NoContent(http.StatusServiceUnavailable) })
	e.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodHead, "/v1/sync", nil))
	e.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("CUSTOM", "/v1/custom", nil))
	assertMetricCount(t, registry, "core_api_http_requests_total", map[string]string{"operation": "/v1/sync", "method": "HEAD", "status_class": "2xx"}, 1)
	assertMetricCount(t, registry, "core_api_http_requests_total", map[string]string{"operation": "/v1/custom", "method": "OTHER", "status_class": "5xx"}, 1)
	for _, method := range []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"} {
		if got := boundedMethod(method); got != method {
			t.Errorf("boundedMethod(%q)=%q", method, got)
		}
	}
	if got := boundedMethod("TRACE"); got != "OTHER" {
		t.Errorf("boundedMethod(TRACE)=%q, want OTHER", got)
	}
}

func assertMetricCount(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string, want uint64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			matches := len(metric.GetLabel()) == len(labels)
			for _, label := range metric.GetLabel() {
				if labels[label.GetName()] != label.GetValue() {
					matches = false
				}
			}
			if !matches {
				continue
			}
			var got uint64
			if metric.GetCounter() != nil {
				got = uint64(metric.GetCounter().GetValue())
			} else {
				got = metric.GetHistogram().GetSampleCount()
			}
			if got != want {
				t.Fatalf("%s labels=%v count=%d, want %d", name, labels, got, want)
			}
			return
		}
	}
	t.Fatalf("metric %s labels=%v not found", name, labels)
}
