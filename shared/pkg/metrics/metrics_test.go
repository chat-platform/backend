package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRegistryInstancesAreIsolated(t *testing.T) {
	first, err := NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	metric := prometheus.NewGauge(prometheus.GaugeOpts{Name: "metrics_registry_isolation_test", Help: "Registry isolation test."})
	if err := first.Register(metric); err != nil {
		t.Fatal(err)
	}
	if err := second.Register(metric); err != nil {
		t.Fatalf("fresh registry unexpectedly shares registrations: %v", err)
	}
}

func TestServerExposesMetricsAndShutsDown(t *testing.T) {
	registry, err := NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	probe := prometheus.NewGauge(prometheus.GaugeOpts{Name: "metrics_server_probe", Help: "Metrics server test probe."})
	probe.Set(7)
	if err := registry.Register(probe); err != nil {
		t.Fatal(err)
	}
	server, err := Start("127.0.0.1:0", registry)
	if err != nil {
		t.Fatal(err)
	}

	response, err := http.Get("http://" + server.listener.Addr().String() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "metrics_server_probe 7") {
		t.Fatalf("unexpected metrics response: status=%d body=%s", response.StatusCode, body)
	}
	rootResponse, err := http.Get("http://" + server.listener.Addr().String() + "/")
	if err != nil {
		t.Fatal(err)
	}
	_ = rootResponse.Body.Close()
	if rootResponse.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected root response status: %d", rootResponse.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown metrics server: %v", err)
	}
	if err := <-server.Done(); err != nil {
		t.Fatalf("serve returned error: %v", err)
	}
}

func TestStartReturnsBindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	registry, err := NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Start(listener.Addr().String(), registry); err == nil {
		t.Fatal("expected duplicate bind to fail")
	}
}
