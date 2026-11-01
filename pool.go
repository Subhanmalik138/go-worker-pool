// Package workerpool is a small, dependency-free concurrent job queue.
//
// A Pool runs a fixed number of worker goroutines that pull jobs from a
// bounded channel. Jobs can be retried with backoff, worker panics are
// recovered and reported as errors instead of crashing the process, and
// the pool shuts down cleanly either by draining the queue or by
// cancelling in-flight work.
package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ErrPoolClosed is returned by Submit after the pool has been shut down.
var ErrPoolClosed = errors.New("workerpool: pool is closed")

// Job is a unit of work. Run is executed by a worker; its context is
// cancelled when the pool is stopped, so long-running jobs should watch
// ctx.Done() and return early.
//
// MaxRetries is the number of extra attempts after the first failure
// (0 means "try once"). Backoff, when set, maps the zero-based retry
// index to a delay; when nil, exponential backoff starting at 100ms
// (capped at 5s) is used.
type Job struct {
	ID         string
	Run        func(ctx context.Context) (any, error)
	MaxRetries int
	Backoff    func(retry int) time.Duration
}

// Result is the outcome of one job. Attempts counts every execution,
// including the first, so a job that succeeded on its second try has
// Attempts == 2 and Err == nil.
type Result struct {
	JobID    string
	Value    any
	Err      error
	Attempts int
}

// Stats is a point-in-time snapshot of pool counters.
type Stats struct {
	Submitted int64
	Completed int64
	Failed    int64
	Retried   int64
}

// Options configures a Pool. Zero values are replaced with defaults:
// Workers defaults to 1, QueueSize defaults to 0 (unbuffered, so Submit
// blocks until a worker picks the job up).
type Options struct {
	Workers   int
	QueueSize int
}

// Pool is a bounded set of workers processing jobs from a queue.
type Pool struct {
	workers  int
	jobs     chan Job
	results  chan Result
	internal chan Result // workers send here; forwardResults relays to results

	ctx    context.Context
	cancel context.CancelFunc

	wg           sync.WaitGroup
	closeMu      sync.Mutex
	closed       bool
	started      bool
	internalOnce sync.Once

	submitted atomic.Int64
	completed atomic.Int64
	failed    atomic.Int64
	retried   atomic.Int64
}

// New creates a pool. It does not start workers; call Start.
func New(opts Options) *Pool {
	if opts.Workers < 1 {
		opts.Workers = 1
	}
	if opts.QueueSize < 0 {
		opts.QueueSize = 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		workers:  opts.Workers,
		jobs:     make(chan Job, opts.QueueSize),
		results:  make(chan Result),
		internal: make(chan Result),
		ctx:      ctx,
		cancel:   cancel,
	}
	// The forwarder owns the public results channel. Workers hand results
	// to it over the internal channel and never block on a slow consumer:
	// the forwarder queues results in a slice and forwards them whenever
	// a receiver is ready. This is what makes Shutdown-before-drain safe
	// — without it, workers stuck sending results would deadlock against
	// Shutdown waiting for the workers to exit.
	go p.forwardResults()
	return p
}

// forwardResults relays results from the internal channel to the public
// one, queueing them in memory while no receiver is ready. It closes the
// public channel once the internal channel is closed and the queue has
// drained, so ranging over Results() always terminates after Shutdown.
func (p *Pool) forwardResults() {
	var queue []Result
	for {
		if len(queue) == 0 {
			res, ok := <-p.internal
			if !ok {
				close(p.results)
				return
			}
			queue = append(queue, res)
			continue
		}
		select {
		case res, ok := <-p.internal:
			if !ok {
				for _, r := range queue {
					p.results <- r
				}
				close(p.results)
				return
			}
			queue = append(queue, res)
		case p.results <- queue[0]:
			queue = queue[1:]
		}
	}
}

// closeInternal closes the internal channel exactly once, signalling
// the forwarder to drain and then close the public results channel.
func (p *Pool) closeInternal() {
	p.internalOnce.Do(func() { close(p.internal) })
}

// Start launches the worker goroutines. It is safe to call more than
// once; only the first call spawns workers.
func (p *Pool) Start() {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	if p.closed || p.started {
		return
	}
	p.started = true
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// Submit queues a job. It blocks until a worker accepts the job or the
// pool is stopped, and returns ErrPoolClosed in the latter case (or if
// Submit is called after Shutdown). Submit must not be called
// concurrently with Shutdown/Stop; if it races one anyway, the send on
// the closed queue is recovered and reported as ErrPoolClosed rather
// than panicking.
func (p *Pool) Submit(job Job) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrPoolClosed
		}
	}()
	p.closeMu.Lock()
	if p.closed {
		p.closeMu.Unlock()
		return ErrPoolClosed
	}
	p.closeMu.Unlock()

	select {
	case p.jobs <- job:
		p.submitted.Add(1)
		return nil
	case <-p.ctx.Done():
		return ErrPoolClosed
	}
}

// Results returns the channel on which completed job results arrive.
// The channel is closed after Shutdown once all workers have exited, so
// callers can range over it to collect every result.
func (p *Pool) Results() <-chan Result {
	return p.results
}

// Stats returns the current counters.
func (p *Pool) Stats() Stats {
	return Stats{
		Submitted: p.submitted.Load(),
		Completed: p.completed.Load(),
		Failed:    p.failed.Load(),
		Retried:   p.retried.Load(),
	}
}

// Shutdown stops accepting new jobs, drains the queued ones, waits for
// in-flight work to finish, and then closes the results channel.
func (p *Pool) Shutdown() {
	p.closeMu.Lock()
	if !p.closed {
		p.closed = true
		close(p.jobs)
	}
	p.closeMu.Unlock()
	p.wg.Wait()
	p.closeInternal()
}

// Stop cancels in-flight work immediately. Jobs still queued are
// discarded; jobs already running see their context cancelled and are
// expected to return promptly. The results channel closes once workers
// have exited.
func (p *Pool) Stop() {
	p.closeMu.Lock()
	if !p.closed {
		p.closed = true
		p.cancel()
	}
	p.closeMu.Unlock()
	p.wg.Wait()
	p.closeInternal()
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		select {
		case job, ok := <-p.jobs:
			if !ok {
				return // drained after Shutdown
			}
			res := p.execute(job)
			select {
			case p.internal <- res:
			case <-p.ctx.Done():
				// Pool was stopped; the forwarder may still deliver
				// queued results, but this one is abandoned.
				return
			}
		case <-p.ctx.Done():
			return // stopped: abandon queued jobs
		}
	}
}

// execute runs a job with retries. A panic inside Run is recovered and
// reported as an error, and counts as a failed attempt (so a panicking
// job is retried like any other failure).
func (p *Pool) execute(job Job) Result {
	backoff := job.Backoff
	if backoff == nil {
		backoff = func(retry int) time.Duration {
			d := 100 * time.Millisecond * time.Duration(1<<uint(retry))
			if d > 5*time.Second {
				return 5 * time.Second
			}
			return d
		}
	}

	var res Result
	res.JobID = job.ID
	for attempt := 0; ; attempt++ {
		res.Attempts = attempt + 1
		value, err := p.runSafely(job)
		if err == nil {
			res.Value = value
			p.completed.Add(1)
			return res
		}
		if attempt >= job.MaxRetries {
			res.Err = err
			p.failed.Add(1)
			return res
		}
		p.retried.Add(1)
		select {
		case <-time.After(backoff(attempt)):
		case <-p.ctx.Done():
			res.Err = p.ctx.Err()
			p.failed.Add(1)
			return res
		}
	}
}

// runSafely executes one attempt, converting a panic into an error.
func (p *Pool) runSafely(job Job) (value any, err error) {
	defer func() {
		if r := recover(); r != nil {
			value = nil
			err = fmt.Errorf("workerpool: job %s panicked: %v", job.ID, r)
		}
	}()
	return job.Run(p.ctx)
}
