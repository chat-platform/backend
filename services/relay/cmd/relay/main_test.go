package main

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chat-platform/backend/shared/pkg/logging"
)

type blockingDispatcher struct {
	active  atomic.Int32
	max     atomic.Int32
	started chan struct{}
}

func (d *blockingDispatcher) RunOnce(ctx context.Context) (int, error) {
	active := d.active.Add(1)
	defer d.active.Add(-1)
	for observed := d.max.Load(); active > observed; observed = d.max.Load() {
		if d.max.CompareAndSwap(observed, active) {
			break
		}
	}
	d.started <- struct{}{}
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestConfiguredWorkersRunConcurrentlyAndStopOnCancellation(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	const workerCount = 3
	ctx, cancel := context.WithCancel(context.Background())
	dispatcher := &blockingDispatcher{started: make(chan struct{}, workerCount)}
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for id := 1; id <= workerCount; id++ {
		go func(id int) {
			defer workers.Done()
			runWorker(ctx, id, time.Millisecond, dispatcher, logger)
		}(id)
	}

	for i := 0; i < workerCount; i++ {
		select {
		case <-dispatcher.started:
		case <-time.After(time.Second):
			cancel()
			workers.Wait()
			t.Fatalf("only %d workers reached RunOnce", i)
		}
	}
	if got := dispatcher.max.Load(); got != workerCount {
		cancel()
		workers.Wait()
		t.Fatalf("maximum concurrent workers=%d, want %d", got, workerCount)
	}

	cancel()
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers did not stop after context cancellation")
	}
}
