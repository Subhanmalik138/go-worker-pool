# go-worker-pool

A small, dependency-free concurrent job queue for Go: a fixed set of
worker goroutines pulling jobs from a bounded queue, with retries,
panic recovery, and clean shutdown.

The problem it solves is the one every Go service hits eventually: you
have a burst of independent tasks (resize images, call downstream APIs,
process files) and you want at most N of them running at once, failed
tasks retried sensibly, and a shutdown path that doesn't strand work or
hang the process.

## Usage

```go
pool := workerpool.New(workerpool.Options{Workers: 4, QueueSize: 100})
pool.Start()

err := pool.Submit(workerpool.Job{
    ID:         "resize-42",
    MaxRetries: 2,
    Run: func(ctx context.Context) (any, error) {
        return resize(ctx, "photo.jpg")
    },
})
// ... submit more jobs, then drain:
pool.Shutdown()

for r := range pool.Results() {
    if r.Err != nil {
        log.Printf("%s failed after %d attempts: %v", r.JobID, r.Attempts, r.Err)
    }
}
fmt.Println(pool.Stats()) // {Submitted Completed Failed Retried}
```

Need to stop now instead of draining? `pool.Stop()` cancels in-flight
work via context and abandons queued jobs.

## Design decisions

1. **Bounded queue, blocking submit.** `QueueSize` caps memory under
   bursts; `Submit` blocks instead of silently dropping work, so
   backpressure propagates to the caller — the behaviour you actually
   want when the producer is faster than the workers.
2. **Retries live in the worker, not the caller.** Each job carries its
   own `MaxRetries` and backoff function (exponential, 100ms → 5s cap,
   by default), so retry policy travels with the job and the caller
   only sees the final result with an accurate attempt count.
3. **Panics become errors.** A panicking job is recovered, reported as
   a failed attempt, and retried like any other failure. One bad job
   must never take down the pool — this is the difference between a
   library and a footgun.
4. **Two shutdown modes, both deadlock-free.** `Shutdown` drains the
   queue and `Stop` cancels contexts for immediate exit; either way,
   `range pool.Results()` terminates once every result is delivered.
   That works because workers hand results to a forwarder goroutine
   with an unbounded in-memory queue instead of sending straight to
   the consumer — so a caller that calls `Shutdown()` before draining
   results can never deadlock workers against the shutdown wait, and a
   `Submit` racing shutdown returns `ErrPoolClosed` instead of
   panicking on a closed channel.

## Run the demo

```sh
go run ./cmd/demo -workers 4 -jobs 12
```

## Run the tests

```sh
go test ./... -race -count=1
```

The suite covers the worker limit (peak concurrency never exceeds the
pool size), retry success and exhaustion, panic recovery with pool
survival, submit-after-shutdown, cancellation via `Stop`, and full
queue draining on `Shutdown` — all under `-race`.

## Tech stack

Go (standard library only, no dependencies). Tested with Go 1.23.
