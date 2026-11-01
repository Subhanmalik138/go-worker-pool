package workerpool

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// collect drains the results channel until it closes.
func collect(t *testing.T, p *Pool) []Result {
	t.Helper()
	var out []Result
	for r := range p.Results() {
		out = append(out, r)
	}
	return out
}

func submitN(t *testing.T, p *Pool, n int, run func(ctx context.Context) (any, error)) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := p.Submit(Job{ID: string(rune('a' + i)), Run: run}); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}
}

func TestPoolRunsJobsAndReportsValues(t *testing.T) {
	p := New(Options{Workers: 3, QueueSize: 10})
	p.Start()
	submitN(t, p, 5, func(ctx context.Context) (any, error) { return 42, nil })
	p.Shutdown()

	results := collect(t, p)
	if len(results) != 5 {
		t.Fatalf("got %d results, want 5", len(results))
	}
	for _, r := range results {
		if r.Value != 42 || r.Err != nil || r.Attempts != 1 {
			t.Errorf("unexpected result %+v", r)
		}
	}
	if s := p.Stats(); s.Completed != 5 || s.Failed != 0 {
		t.Errorf("unexpected stats %+v", s)
	}
}

func TestPoolRespectsWorkerLimit(t *testing.T) {
	var current, maxSeen atomic.Int64
	p := New(Options{Workers: 3, QueueSize: 20})
	p.Start()
	submitN(t, p, 12, func(ctx context.Context) (any, error) {
		c := current.Add(1)
		for {
			m := maxSeen.Load()
			if c <= m || maxSeen.CompareAndSwap(m, c) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		current.Add(-1)
		return nil, nil
	})
	p.Shutdown()
	collect(t, p)
	if maxSeen.Load() > 3 {
		t.Errorf("max concurrency %d exceeded worker limit 3", maxSeen.Load())
	}
	if maxSeen.Load() < 2 {
		t.Errorf("max concurrency %d: jobs did not run in parallel", maxSeen.Load())
	}
}

func TestRetrySucceedsAfterFailures(t *testing.T) {
	var attempts atomic.Int64
	p := New(Options{Workers: 1, QueueSize: 1})
	p.Start()
	err := p.Submit(Job{
		ID:         "flaky",
		MaxRetries: 3,
		Backoff:    func(retry int) time.Duration { return time.Millisecond },
		Run: func(ctx context.Context) (any, error) {
			if attempts.Add(1) < 3 {
				return nil, errors.New("transient failure")
			}
			return "ok", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.Shutdown()
	results := collect(t, p)
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Value != "ok" || results[0].Err != nil || results[0].Attempts != 3 {
		t.Errorf("unexpected result %+v", results[0])
	}
	if s := p.Stats(); s.Retried != 2 {
		t.Errorf("retried = %d, want 2", s.Retried)
	}
}

func TestRetryExhaustionReportsError(t *testing.T) {
	p := New(Options{Workers: 1, QueueSize: 1})
	p.Start()
	err := p.Submit(Job{
		ID:         "always-fails",
		MaxRetries: 2,
		Backoff:    func(retry int) time.Duration { return time.Millisecond },
		Run:        func(ctx context.Context) (any, error) { return nil, errors.New("boom") },
	})
	if err != nil {
		t.Fatal(err)
	}
	p.Shutdown()
	results := collect(t, p)
	if len(results) != 1 || results[0].Err == nil || results[0].Attempts != 3 {
		t.Errorf("unexpected result %+v", results)
	}
	if s := p.Stats(); s.Failed != 1 {
		t.Errorf("failed = %d, want 1", s.Failed)
	}
}

func TestPanicRecovery(t *testing.T) {
	p := New(Options{Workers: 1, QueueSize: 1})
	p.Start()
	err := p.Submit(Job{
		ID:  "panicker",
		Run: func(ctx context.Context) (any, error) { panic("kaboom") },
	})
	if err != nil {
		t.Fatal(err)
	}
	// The pool must survive the panic and still process the next job.
	err = p.Submit(Job{
		ID:  "after",
		Run: func(ctx context.Context) (any, error) { return "alive", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	p.Shutdown()
	results := collect(t, p)
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	if results[0].Err == nil || !strings.Contains(results[0].Err.Error(), "panicked") {
		t.Errorf("expected panic error, got %+v", results[0])
	}
	if results[1].Value != "alive" {
		t.Errorf("pool did not survive panic: %+v", results[1])
	}
}

func TestSubmitAfterShutdown(t *testing.T) {
	p := New(Options{Workers: 1})
	p.Start()
	p.Shutdown()
	if err := p.Submit(Job{Run: func(ctx context.Context) (any, error) { return nil, nil }}); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Submit after Shutdown = %v, want ErrPoolClosed", err)
	}
}

func TestStopCancelsInFlightWork(t *testing.T) {
	started := make(chan struct{})
	var once atomic.Bool
	p := New(Options{Workers: 1, QueueSize: 10})
	p.Start()
	err := p.Submit(Job{
		ID: "long",
		Run: func(ctx context.Context) (any, error) {
			if once.CompareAndSwap(false, true) {
				close(started)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	done := make(chan struct{})
	go func() {
		p.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return: worker stuck on cancelled job")
	}
}

func TestShutdownDrainsQueue(t *testing.T) {
	p := New(Options{Workers: 2, QueueSize: 50})
	p.Start()
	submitN(t, p, 20, func(ctx context.Context) (any, error) { return nil, nil })
	p.Shutdown()
	if got := len(collect(t, p)); got != 20 {
		t.Errorf("got %d results, want 20 (queue not drained)", got)
	}
}
