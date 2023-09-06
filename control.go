package rsmap

import (
	"context"
	"errors"
	"sync"
	"time"
)

// TODO
// * status for init failure
// * timeout for init and acquisition

type initEvent string

const (
	initEventStarted   initEvent = "started"
	initEventCompleted initEvent = "completed"
)

var errRecordNotFound = errors.New("record not found on key value store")

type (
	keyValueStore[T any] interface {
		get(name string) (*T, error)
		set(name string, obj *T) error
		forEach(fn func(name string, obj *T) error) error
	}

	// Control init status and persistence.
	initController struct {
		_kv        keyValueStore[initRecord]
		_resources sync.Map
	}

	initRecord struct {
		Logs []initLog `json:"logs"`
	}

	initLog struct {
		Event     initEvent `json:"event"`
		Operator  string    `json:"operator"`
		Timestamp int64     `json:"ts,string"`
	}
)

func loadInitController(store keyValueStore[initRecord]) (*initController, error) {
	c := &initController{
		_kv: store,
	}
	err := c._kv.forEach(func(name string, obj *initRecord) error {
		// Get init status and operator.
		completed, operator := func() (bool, string) {
			if len(obj.Logs) == 0 {
				return false, "" // Basically, it's impossible path.
			}
			last := obj.Logs[len(obj.Logs)-1]
			return last.Event == initEventCompleted, last.Operator
		}()

		ctl := newInitCtl(completed)
		if !completed {
			_ = ctl.tryInit(
				context.Background(),
				operator,
				func(try bool) error { return nil },
			)
		}
		c._resources.Store(name, ctl)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *initController) tryInit(ctx context.Context, resourceName, operator string) (try bool, _ error) {
	v, _ := c._resources.LoadOrStore(resourceName, newInitCtl(false))
	ctl := v.(*initCtl)

	// If the operator acquires lock for init but the fact is not recognized by operator,
	// give a second chance to do init.
	if !ctl._completed && ctl._operator == operator {
		return true, nil
	}

	err := ctl.tryInit(ctx, operator, func(_try bool) error {
		try = _try
		if !try {
			return nil
		}

		// Update data on key value store.
		r, err := c._kv.get(resourceName)
		if errors.Is(err, errRecordNotFound) {
			r = &initRecord{}
		} else if err != nil {
			return err
		}
		r.Logs = append(r.Logs, initLog{
			Event:     initEventStarted,
			Operator:  operator,
			Timestamp: time.Now().UnixNano(),
		})
		return c._kv.set(resourceName, r)
	})
	if err != nil {
		return false, err
	}

	return try, nil
}

func (c *initController) complete(resourceName, operator string) error {
	v, found := c._resources.Load(resourceName)
	if !found {
		return errors.New("resource not found")
	}
	ctl := v.(*initCtl)

	return ctl.complete(operator, func() error {
		r, err := c._kv.get(resourceName)
		if err != nil {
			return err
		}
		r.Logs = append(r.Logs, initLog{
			Event:     initEventCompleted,
			Operator:  operator,
			Timestamp: time.Now().UnixNano(),
		})
		return c._kv.set(resourceName, r)
	})
}

type acquireEvent string

const (
	acquireEventAcquired acquireEvent = "acquired"
	acquireEventReleased acquireEvent = "released"
)

type (
	// Control acquisition status and persistence.
	acquireController struct {
		_kv        keyValueStore[acquireRecord]
		_resources sync.Map
	}

	acquireRecord struct {
		Max  int64        `json:"max"`
		Logs []acquireLog `json:"logs"`
	}

	acquireLog struct {
		Event     acquireEvent `json:"event"`
		N         int64        `json:"n,omitempty"`
		Operator  string       `json:"operator"`
		Timestamp int64        `json:"ts,string"`
	}
)

func loadAcquireController(store keyValueStore[acquireRecord]) (*acquireController, error) {
	c := &acquireController{
		_kv: store,
	}

	err := store.forEach(func(name string, obj *acquireRecord) error {
		acquired := map[string]int64{}
		// Replay stored acquisitions of the resource.
		for _, log := range obj.Logs {
			switch log.Event {
			case acquireEventAcquired:
				// Consecutive acquisition is not recorded.
				// So, we can skip the check of existing value.
				//
				// See: `(*acquireCtl).acquire()`
				acquired[log.Operator] = log.N
			case acquireEventReleased:
				// We assume that acquisition log is already processed.
				delete(acquired, log.Operator)
			}
		}
		// Set replayed acquireCtl.
		c._resources.Store(
			name,
			newAcquireCtl(obj.Max, acquired),
		)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *acquireController) acquire(ctx context.Context, resourceName, operator string, max int64, exclusive bool) error {
	v, _ := c._resources.LoadOrStore(resourceName, newAcquireCtl(max, map[string]int64{}))
	ctl := v.(*acquireCtl)

	n, err := ctl.acquire(ctx, operator, exclusive)
	if err != nil {
		return err
	}
	if n == 0 {
		// Due to trial of consecutive acquisition, not acquired actually.
		return nil
	}

	r, err := c._kv.get(resourceName)
	if errors.Is(err, errRecordNotFound) {
		r = &acquireRecord{
			Max: max,
		}
	} else if err != nil {
		return err
	}
	r.Logs = append(r.Logs, acquireLog{
		Event:     acquireEventAcquired,
		N:         n,
		Operator:  operator,
		Timestamp: time.Now().UnixNano(),
	})
	return c._kv.set(resourceName, r)
}

func (c *acquireController) release(resourceName, operator string) error {
	v, found := c._resources.Load(resourceName)
	if !found {
		// If the resource not found, return without error.
		return nil
	}
	ctl := v.(*acquireCtl)
	if released := ctl.release(operator); !released {
		// If not acquired, return without error.
		return nil
	}

	r, err := c._kv.get(resourceName)
	if err != nil {
		return err
	}
	r.Logs = append(r.Logs, acquireLog{
		Event:     acquireEventReleased,
		N:         0,
		Operator:  operator,
		Timestamp: time.Now().UnixNano(),
	})
	return c._kv.set(resourceName, r)
}
