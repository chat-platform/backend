// Package metrics provides isolated Prometheus registries and a dedicated
// metrics HTTP server for backend services.
package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRegistry creates a fresh registry with Go runtime and process collectors.
// Each service process should use its own registry instead of the global one.
func NewRegistry() (*prometheus.Registry, error) {
	registry := prometheus.NewRegistry()
	for _, collector := range []prometheus.Collector{
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	} {
		if err := registry.Register(collector); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

// DefaultBuckets returns a copy of Prometheus's standard latency buckets.
func DefaultBuckets() []float64 {
	return append([]float64(nil), prometheus.DefBuckets...)
}

// Server is a dedicated HTTP server that exposes only /metrics.
type Server struct {
	server      *http.Server
	listener    net.Listener
	done        chan error
	shutdown    sync.Once
	shutdownErr error
}

// Start binds addr synchronously, then starts serving the provided registry.
// It returns a bind error to the caller before the service is marked ready.
func Start(addr string, gatherer prometheus.Gatherer) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	server := &Server{
		server:   &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second},
		listener: listener,
		done:     make(chan error, 1),
	}
	go func() {
		err := server.server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		server.done <- err
		close(server.done)
	}()
	return server, nil
}

// Done returns the terminal error from the HTTP Serve loop. A nil error means
// the listener shut down cleanly.
func (s *Server) Done() <-chan error { return s.done }

// Shutdown gracefully stops serving and waits for the Serve loop. If ctx
// expires, the listener is force-closed and the context error is returned.
func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdown.Do(func() {
		s.shutdownErr = s.server.Shutdown(ctx)
		if s.shutdownErr != nil {
			_ = s.server.Close()
		}
		select {
		case serveErr := <-s.done:
			if s.shutdownErr == nil {
				s.shutdownErr = serveErr
			}
		case <-ctx.Done():
			_ = s.server.Close()
			if s.shutdownErr == nil {
				s.shutdownErr = ctx.Err()
			}
		}
	})
	return s.shutdownErr
}
