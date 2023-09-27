package rendezvous

import (
	"container/list"
	"context"
	"sync"
	"time"
)

type (
	Builder struct {
		_l *list.List
		_m map[string]*list.Element
	}

	LimitedTermQueue interface {
		Dequeue(key string, fn func(bool))
	}
)

func NewBuilder() *Builder {
	return &Builder{
		_l: list.New(),
		_m: map[string]*list.Element{},
	}
}

func (b *Builder) Add(s string) {
	if _, ok := b._m[s]; !ok {
		b._m[s] = b._l.PushBack(s)
	}
}

func (b *Builder) Remove(s string) {
	if e, ok := b._m[s]; ok {
		b._l.Remove(e)
	}
}

type limitedTermQueue struct {
	_l *list.List
	_m map[string]*list.Element

	_cond *cancelableCond[struct{}]
	_done <-chan struct{}
}

type emptyQueue struct{}

func (b *Builder) Start(timeout time.Duration) LimitedTermQueue {
	if b._l.Len() == 0 {
		return &emptyQueue{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	done := ctx.Done()
	q := &limitedTermQueue{
		_l:    b._l,
		_m:    b._m,
		_done: done,
		_cond: newCancelableCond(&sync.Mutex{}, done),
	}
	b._l = list.New()
	b._m = map[string]*list.Element{}

	go func() {
		defer cancel()

		q._cond.L.Lock()
		defer q._cond.L.Unlock()
		for {
			if q._l.Len() == 0 {
				cancel()
				// return
			}
			if cont := q._cond.wait(); !cont {
				return
			}
		}
	}()

	return q
}

func (q *limitedTermQueue) Dequeue(s string, fn func(dequeue bool)) {
	select {
	case <-q._done:
		fn(false)
		return
	default:
	}

	q._cond.L.Lock()
	for {
		if e := q._l.Front(); e != nil && e.Value == s {
			q._l.Remove(e)
			fn(true)
			q._cond.broadcast()
			break
		}
		if cont := q._cond.wait(); !cont {
			fn(false)
			break
		}
	}
	q._cond.L.Unlock()
}

func (emptyQueue) Dequeue(_ string, fn func(bool)) {
	fn(false)
}
