package engine

import "context"

// Semaphore limits concurrent access via a buffered channel.
// Used to enforce concurrency.max_runs at the DAG level.
type Semaphore struct {
	ch chan struct{}
}

func NewSemaphore(n int) *Semaphore {
	return &Semaphore{ch: make(chan struct{}, n)}
}

// Acquire blocks until a slot is available or ctx is cancelled.
func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release frees a slot.
func (s *Semaphore) Release() {
	<-s.ch
}
