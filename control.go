package rsmap

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/daichitakahashi/rsmap/logs"
)

// TODO
// * status for init failure
// * timeout for init and acquisition

type initController struct {
	_kv        logs.ResourceRecordStore[logs.InitRecord] // TODO: fix name
	_resources sync.Map
}

func loadInitController(store logs.ResourceRecordStore[logs.InitRecord]) (*initController, error) {
	c := &initController{
		_kv: store,
	}
	err := c._kv.ForEach(func(name string, obj *logs.InitRecord) error {
		// Get init status and operator.
		completed, operator := func() (bool, string) {
			if len(obj.Logs) == 0 {
				return false, "" // Basically, it's impossible path.
			}
			last := obj.Logs[len(obj.Logs)-1]
			return last.Event == logs.InitEventCompleted, last.Operator
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
	if !ctl.completed.Load() && ctl.operator == operator {
		return true, nil
	}

	err := ctl.tryInit(ctx, operator, func(_try bool) error {
		try = _try
		if !try {
			return nil
		}

		// Update data on key value store.
		return c._kv.Put(resourceName, func(r *logs.InitRecord, _ bool) {
			r.Logs = append(r.Logs, logs.InitLog{
				Event:     logs.InitEventStarted,
				Operator:  operator,
				Timestamp: time.Now().UnixNano(),
			})
		})
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
		return c._kv.Put(resourceName, func(r *logs.InitRecord, _ bool) {
			r.Logs = append(r.Logs, logs.InitLog{
				Event:     logs.InitEventCompleted,
				Operator:  operator,
				Timestamp: time.Now().UnixNano(),
			})
		})
	})
}

// Control acquisition status and persistence.
type acquireController struct {
	_kv        logs.ResourceRecordStore[logs.AcquireRecord]
	_resources sync.Map
}

func loadAcquireController(store logs.ResourceRecordStore[logs.AcquireRecord]) (*acquireController, error) {
	c := &acquireController{
		_kv: store,
	}

	err := store.ForEach(func(name string, obj *logs.AcquireRecord) error {
		acquired := map[string]int64{}
		// Replay stored acquisitions of the resource.
		for _, log := range obj.Logs {
			switch log.Event {
			case logs.AcquireEventAcquired:
				// Consecutive acquisition is not recorded.
				// So, we can skip the check of existing value.
				//
				// See: `(*acquireCtl).acquire()`
				acquired[log.Operator] = log.N
			case logs.AcquireEventReleased:
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

	return c._kv.Put(resourceName, func(r *logs.AcquireRecord, update bool) {
		// Initial acquisition.
		if !update {
			r.Max = max
		}
		r.Logs = append(r.Logs, logs.AcquireLog{
			Event:     logs.AcquireEventAcquired,
			N:         n,
			Operator:  operator,
			Timestamp: time.Now().UnixNano(),
		})
	})
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

	return c._kv.Put(resourceName, func(r *logs.AcquireRecord, _ bool) {
		r.Logs = append(r.Logs, logs.AcquireLog{
			Event:     logs.AcquireEventReleased,
			N:         0,
			Operator:  operator,
			Timestamp: time.Now().UnixNano(),
		})
	})
}
