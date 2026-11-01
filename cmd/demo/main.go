// Command demo runs a small batch of fake image-resize jobs through a
// worker pool, showing retries, results, and pool statistics.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"time"

	workerpool "github.com/Subhanmalik138/go-worker-pool"
)

func main() {
	workers := flag.Int("workers", 4, "number of worker goroutines")
	jobs := flag.Int("jobs", 12, "number of jobs to run")
	flag.Parse()

	pool := workerpool.New(workerpool.Options{Workers: *workers, QueueSize: *jobs})
	pool.Start()

	go func() {
		for i := 0; i < *jobs; i++ {
			id := fmt.Sprintf("image-%02d.jpg", i)
			attempt := 0
			err := pool.Submit(workerpool.Job{
				ID:         id,
				MaxRetries: 2,
				Run: func(ctx context.Context) (any, error) {
					attempt++
					// Simulate work; every third job fails once (flaky I/O).
					time.Sleep(time.Duration(50+rand.Intn(150)) * time.Millisecond)
					if i%3 == 0 && attempt == 1 {
						return nil, errors.New("transient read error")
					}
					return fmt.Sprintf("%s resized", id), nil
				},
			})
			if err != nil {
				fmt.Println("submit error:", err)
			}
		}
		pool.Shutdown()
	}()

	for r := range pool.Results() {
		if r.Err != nil {
			fmt.Printf("[FAIL] %s after %d attempt(s): %v\n", r.JobID, r.Attempts, r.Err)
		} else {
			fmt.Printf("[OK]   %s (%d attempt(s)): %v\n", r.JobID, r.Attempts, r.Value)
		}
	}
	fmt.Printf("\nStats: %+v\n", pool.Stats())
}
