package ctl

import (
	"context"
	"sync"
)

type (
	// AcquisitionCtl is a primitive for controlling acquisition status.
	AcquisitionCtl struct {
		_sem      *semaphore
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
	sem := newSemaphore(max)

	// Replay acquisitions.
	for _, n := range acquired {
		if n > 0 {
			<-sem.acquire(context.Background(), n, nil)
		}
	}

	return &AcquisitionCtl{
		_sem:      sem,
		_max:      max,
		_acquired: acquired,
	}
}

func (c *AcquisitionCtl) Acquired(operator string) bool {
	c._m.Lock()
	defer c._m.Unlock()

	_, ok := c._acquired[operator]
	return ok
}

// Acquire acquires exclusive/shared lock.
func (c *AcquisitionCtl) Acquire(ctx context.Context, operator string, exclusive bool) (<-chan AcquisitionResult, bool) {
	c._m.Lock()
	defer c._m.Unlock()

	_, ok := c._acquired[operator]
	if ok {
		// If already acquired by this operator, return without acquisition.
		// MEMO: Do we need to await ongoing acquisition?
		return nil, false
	}

	n := int64(1)
	if exclusive {
		n = c._max
	}
	// Record acquired operator.
	c._acquired[operator] = n

	return c._sem.acquire(ctx, n, func(r AcquisitionResult) {
		if r.Err != nil { // On cancel.
			c._m.Lock()
			delete(c._acquired, operator)
			c._m.Unlock()
		}
	}), true
}

// Release releases acquired lock.
func (c *AcquisitionCtl) Release(operator string) bool {
	c._m.Lock()
	n, ok := c._acquired[operator]
	if !ok {
		c._m.Unlock()
		// If not acquired, return without error.
		return false
	}
	delete(c._acquired, operator)
	c._m.Unlock()

	c._sem.release(n) // Do release outside of Lock/Unlock scope.
	return true
}
