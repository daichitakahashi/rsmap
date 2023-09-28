package ctl

import (
	"container/list"
	"context"
	"fmt"
	"sync"
)

type (
	// Channel based semaphore.
	semaphore struct {
		_size    int64
		_cur     int64
		_mu      sync.Mutex
		_waiters *list.List
	}

	waiter struct {
		_n     int64
		_ready chan<- struct{}
		_done  <-chan struct{}
	}
)

func newSemaphore(max int64) *semaphore {
	return &semaphore{
		_size:    max,
		_waiters: list.New(),
	}
}

func (s *semaphore) acquire(ctx context.Context, n int64, hook func(r AcquisitionResult)) <-chan AcquisitionResult {
	s._mu.Lock()
	defer s._mu.Unlock()

	if s._size-s._cur >= n && s._waiters.Len() == 0 {
		s._cur += n

		r := AcquisitionResult{
			Acquired: n,
		}
		if hook != nil {
			hook(r)
		}
		ch := make(chan AcquisitionResult, 1)
		ch <- r
		return ch
	}
	if n > s._size {
		panic(fmt.Sprintf("semaphore: acquire more than %d(%d)", s._size, n))
	}

	var (
		ready  = make(chan struct{})
		result = make(chan AcquisitionResult, 1)
		done   = ctx.Done()
	)
	s._waiters.PushBack(waiter{
		_n:     n,
		_ready: ready,
		_done:  done,
	})

	begin := make(chan struct{})
	go func() {
		close(begin)
		var r AcquisitionResult
		select {
		case <-done:
			r.Err = ctx.Err()
			defer func() {
				s._mu.Lock()
				s._notifyWaiters()
				s._mu.Unlock()
			}()
		case <-ready:
			r.Acquired = n
		}

		if hook != nil {
			hook(r)
		}
		result <- r
	}()
	<-begin

	return result
}

func (s *semaphore) release(n int64) {
	s._mu.Lock()
	defer s._mu.Unlock()

	s._cur -= n
	if s._cur < 0 {
		panic("semaphore: released more than held")
	}
	s._notifyWaiters()
}

func (s *semaphore) _notifyWaiters() {
LOOP:
	for {
		next := s._waiters.Front()
		if next == nil {
			break // No more waiters blocked.
		}

		w := next.Value.(waiter)
		if s._size-s._cur < w._n { // Insufficient slot.
			select {
			case <-w._done: // Already canceled, remove and skip it.
				s._waiters.Remove(next)
				continue LOOP
			default:
				break LOOP // Wait next notification.
			}
		}

		select {
		case w._ready <- struct{}{}:
			s._cur += w._n
		default:
			// Already canceled.
		}
		s._waiters.Remove(next)
	}
}
