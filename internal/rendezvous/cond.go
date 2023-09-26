package rendezvous

import "sync"

type cancelableCond[T any] struct {
	L       sync.Locker
	_sig    chan struct{}
	_sigM   sync.RWMutex // locker for refreshing _sig
	_cancel <-chan T
}

func newCancelableCond[T any](l sync.Locker, cancel <-chan T) *cancelableCond[T] {
	return &cancelableCond[T]{
		L:       l,
		_sig:    make(chan struct{}),
		_cancel: cancel,
	}
}

func (c *cancelableCond[T]) wait() bool {
	c._sigM.RLock()
	s := c._sig
	c._sigM.RUnlock()

	c.L.Unlock()
	defer c.L.Lock()
	select {
	case <-s:
		return true
	case <-c._cancel:
		return false
	}
}

func (c *cancelableCond[T]) broadcast() {
	c._sigM.Lock()
	close(c._sig)
	c._sig = make(chan struct{})
	c._sigM.Unlock()
}
