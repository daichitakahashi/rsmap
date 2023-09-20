package rsmap

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/daichitakahashi/rsmap/logs"
)

// TODO
// * timeout for init and acquisition

type initController struct {
	_store     logs.ResourceRecordStore[logs.InitRecord]
	_resources sync.Map
}

func loadInitController(store logs.ResourceRecordStore[logs.InitRecord]) (*initController, error) {
	c := &initController{
		_store: store,
	}
	err := c._store.ForEach(func(name string, obj *logs.InitRecord) error {
		if len(obj.Logs) == 0 {
			return nil // Impossible path.
		}

		// Get last init status and operator.
		last := obj.Logs[len(obj.Logs)-1]
		if last.Event == logs.InitEventFailed {
			return nil // Former try is failed and anyone haven't started next try yet.
		}

		completed := last.Event == logs.InitEventCompleted
		ctl := newInitCtl(completed)
		if !completed {
			_ = ctl.tryInit(
				context.Background(),
				last.Context.String(),
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

func (c *initController) tryInit(ctx context.Context, resourceName string, operator logs.CallerContext) (try bool, _ error) {
	v, _ := c._resources.LoadOrStore(resourceName, newInitCtl(false))
	ctl := v.(*initCtl)

	op := operator.String()

	// If the operator acquires lock for init but the fact is not recognized by operator,
	// give a second chance to do init.
	if !ctl.completed.Load() && ctl.operator == op {
		return true, nil
	}

	err := ctl.tryInit(ctx, op, func(_try bool) error {
		try = _try
		if !try {
			return nil
		}

		// Update data on key value store.
		return c._store.Put(resourceName, func(r *logs.InitRecord, _ bool) {
			r.Logs = append(r.Logs, logs.InitLog{
				Event:     logs.InitEventStarted,
				Context:   operator,
				Timestamp: time.Now().UnixNano(),
			})
		})
	})
	if err != nil {
		return false, err
	}

	return try, nil
}

func (c *initController) complete(resourceName string, operator logs.CallerContext) error {
	v, found := c._resources.Load(resourceName)
	if !found {
		return errors.New("resource not found")
	}
	ctl := v.(*initCtl)

	return ctl.complete(operator.String(), func() error {
		return c._store.Put(resourceName, func(r *logs.InitRecord, _ bool) {
			r.Logs = append(r.Logs, logs.InitLog{
				Event:     logs.InitEventCompleted,
				Context:   operator,
				Timestamp: time.Now().UnixNano(),
			})
		})
	})
}

func (c *initController) fail(resourceName string, operator logs.CallerContext) error {
	v, found := c._resources.Load(resourceName)
	if !found {
		return errors.New("resource not found")
	}
	ctl := v.(*initCtl)

	return ctl.fail(operator.String(), func() error {
		return c._store.Put(resourceName, func(r *logs.InitRecord, _ bool) {
			r.Logs = append(r.Logs, logs.InitLog{
				Event:     logs.InitEventFailed,
				Context:   operator,
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
				acquired[log.Context.String()] = log.N
			case logs.AcquireEventReleased:
				// We assume that acquisition log is already processed.
				delete(acquired, log.Context.String())
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

func (c *acquireController) acquire(ctx context.Context, resourceName string, operator logs.CallerContext, max int64, exclusive bool) error {
	v, _ := c._resources.LoadOrStore(resourceName, newAcquireCtl(max, map[string]int64{}))
	ctl := v.(*acquireCtl)

	n, err := ctl.acquire(ctx, operator.String(), exclusive)
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
			Context:   operator,
			Timestamp: time.Now().UnixNano(),
		})
	})
}

func (c *acquireController) release(resourceName string, operator logs.CallerContext) error {
	v, found := c._resources.Load(resourceName)
	if !found {
		// If the resource not found, return without error.
		return nil
	}
	ctl := v.(*acquireCtl)
	if released := ctl.release(operator.String()); !released {
		// If not acquired, return without error.
		return nil
	}

	return c._kv.Put(resourceName, func(r *logs.AcquireRecord, _ bool) {
		r.Logs = append(r.Logs, logs.AcquireLog{
			Event:     logs.AcquireEventReleased,
			N:         0,
			Context:   operator,
			Timestamp: time.Now().UnixNano(),
		})
	})
}
