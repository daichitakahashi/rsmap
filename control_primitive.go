package rsmap

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

// Primitive for controlling init status.
type initCtl struct {
	_lock chan struct{}
	// _t      *time.Timer   // For timeout of started init operation.
	completed *atomic.Bool
	operator  string
}

// Create new initCtl.
func newInitCtl(completed bool) *initCtl {
	var c atomic.Bool
	if completed {
		c.Store(completed)
	}
	return &initCtl{
		_lock:     make(chan struct{}, 1), // allocate minimum buffer
		completed: &c,
	}
}

// Try to acquire lock and start init operation.
// When operation is already completed, 'try' will be false.
func (c *initCtl) tryInit(ctx context.Context, operator string, fn func(try bool) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c._lock <- struct{}{}:
		if c.completed.Load() {
			if err := fn(false); err != nil {
				return err
			}
			<-c._lock // Release.
			return nil
		}

		// Set current operator.
		c.operator = operator

		if err := fn(true); err != nil {
			return err
		}

		// Keep locked.
		// TODO(timeout): Set timeout.
		return nil
	}
}

// Mark init operation as completed.
func (c *initCtl) complete(operator string, fn func() error) error {
	if c.operator != operator {
		return errors.New("invalid operation")
	}

	if err := fn(); err != nil {
		return err
	}

	// Update status.
	c.completed.Store(true)

	<-c._lock // Release.
	return nil
}

// Mark init operation by operator as failed.
func (c *initCtl) fail(operator string, fn func() error) error {
	if c.operator != operator {
		return errors.New("invalid operation")
	}

	if err := fn(); err != nil {
		return err
	}

	<-c._lock // Release.
	return nil
}

// Primitive for controlling acquisition status.
type acquireCtl struct {
	_sem      *semaphore.Weighted
	_max      int64
	_m        sync.Mutex
	_acquired map[string]int64
}

func newAcquireCtl(max int64, acquired map[string]int64) *acquireCtl {
	sem := semaphore.NewWeighted(max)
	// Replay acquisitions.
	for _, n := range acquired {
		if n > 0 {
			_ = sem.Acquire(context.Background(), n)
		}
	}

	return &acquireCtl{
		_sem:      sem,
		_max:      max,
		_acquired: acquired,
	}
}

// Acquire exclusive/shared lock.
func (c *acquireCtl) acquire(ctx context.Context, operator string, exclusive bool) (int64, error) {
	c._m.Lock()
	_, ok := c._acquired[operator]
	c._m.Unlock()
	if ok {
		// If already acquired by this operator, return 0.
		return 0, nil
	}

	n := int64(1)
	if exclusive {
		n = c._max
	}
	if err := c._sem.Acquire(ctx, n); err != nil {
		return 0, err
	}

	// Record acquired operator.
	c._acquired[operator] = n
	return n, nil
}

func (c *acquireCtl) release(operator string) bool {
	c._m.Lock()
	n, ok := c._acquired[operator]
	c._m.Unlock()
	if !ok {
		// If not acquired, return without error.
		return false
	}
	delete(c._acquired, operator)
	c._sem.Release(n)
	return true
}
