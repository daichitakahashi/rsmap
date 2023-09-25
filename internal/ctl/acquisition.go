package ctl

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

type (
	// AcquisitionCtl is a primitive for controlling acquisition status.
	AcquisitionCtl struct {
		_sem      *semaphore.Weighted
		_max      int64
		_m        sync.Mutex
		_acquired map[string]int64
	}

	AcquisitionResult struct {
		Acquired int64
		Err      error
	}
)

// NewAcquisitionCtl creates new AcquisitionCtl.
func NewAcquisitionCtl(max int64, acquired map[string]int64) *AcquisitionCtl {
	sem := semaphore.NewWeighted(max)
	// Replay acquisitions.
	for _, n := range acquired {
		if n > 0 {
			_ = sem.Acquire(context.Background(), n)
		}
	}

	return &AcquisitionCtl{
		_sem:      sem,
		_max:      max,
		_acquired: acquired,
	}
}

// Acquire acquires exclusive/shared lock.
func (c *AcquisitionCtl) Acquire(ctx context.Context, operator string, exclusive bool) <-chan AcquisitionResult {
	result := make(chan AcquisitionResult, 1)

	c._m.Lock()
	_, ok := c._acquired[operator]
	if ok {
		c._m.Unlock()
		result <- AcquisitionResult{}
		// If already acquired by this operator, return 0.
		return result
	}

	n := int64(1)
	if exclusive {
		n = c._max
	}
	// Record acquired operator.
	c._acquired[operator] = n
	c._m.Unlock()

	go func() {
		if err := c._sem.Acquire(ctx, n); err != nil {
			c._m.Lock()
			delete(c._acquired, operator)
			c._m.Unlock()

			result <- AcquisitionResult{
				Err: err,
			}
			return
		}
		result <- AcquisitionResult{
			Acquired: n,
		}
	}()

	return result
}

// Release releases acquired lock.
func (c *AcquisitionCtl) Release(operator string) bool {
	c._m.Lock()
	defer c._m.Unlock()
	n, ok := c._acquired[operator]
	if !ok {
		// If not acquired, return without error.
		return false
	}
	delete(c._acquired, operator)
	c._sem.Release(n)
	return true
}
