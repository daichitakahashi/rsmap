package rsmap

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/daichitakahashi/rsmap/internal/ctl"
	logsv1 "github.com/daichitakahashi/rsmap/internal/proto/logs/v1"
	"github.com/daichitakahashi/rsmap/logs"
)

// TODO
// * timeout for init and acquisition

type initController struct {
	_store     logs.ResourceRecordStore[logsv1.InitRecord]
	_resources sync.Map
}

func loadInitController(store logs.ResourceRecordStore[logsv1.InitRecord]) (*initController, error) {
	c := &initController{
		_store: store,
	}
	err := c._store.ForEach(func(name string, obj *logsv1.InitRecord) error {
		if len(obj.Logs) == 0 {
			return nil // Impossible path.
		}

		// Get last init status and operator.
		last := obj.Logs[len(obj.Logs)-1]
		if last.Event == logsv1.InitEvent_INIT_EVENT_FAILED {
			return nil // Former try is failed and anyone haven't started next try yet.
		}

		completed := last.Event == logsv1.InitEvent_INIT_EVENT_COMPLETED
		ctl := ctl.NewInitCtl(completed)
		if !completed {
			<-ctl.TryInit(
				context.Background(),
				logs.CallerContext(last.Context).String(),
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

func (c *initController) tryInit(ctx context.Context, resourceName string, operator logs.CallerContext) (bool, error) {
	v, _ := c._resources.LoadOrStore(resourceName, ctl.NewInitCtl(false))
	ctl := v.(*ctl.InitCtl)

	result := <-ctl.TryInit(ctx, operator.String())
	if result.Err != nil {
		return false, result.Err
	}
	if !result.Try {
		return false, nil
	}

	if result.Initiated {
		// Update data on key value store.
		err := c._store.Put(resourceName, func(r *logsv1.InitRecord, _ bool) {
			r.Logs = append(r.Logs, &logsv1.InitLog{
				Event:     logsv1.InitEvent_INIT_EVENT_STARTED,
				Context:   operator,
				Timestamp: time.Now().UnixNano(),
			})
		})
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (c *initController) complete(resourceName string, operator logs.CallerContext) error {
	v, found := c._resources.Load(resourceName)
	if !found {
		return errors.New("resource not found")
	}
	ctl := v.(*ctl.InitCtl)

	err := ctl.Complete(operator.String())
	if err != nil {
		return err
	}

	return c._store.Put(resourceName, func(r *logsv1.InitRecord, _ bool) {
		r.Logs = append(r.Logs, &logsv1.InitLog{
			Event:     logsv1.InitEvent_INIT_EVENT_COMPLETED,
			Context:   operator,
			Timestamp: time.Now().UnixNano(),
		})
	})
}

func (c *initController) fail(resourceName string, operator logs.CallerContext) error {
	v, found := c._resources.Load(resourceName)
	if !found {
		return errors.New("resource not found")
	}
	ctl := v.(*ctl.InitCtl)

	err := ctl.Fail(operator.String())
	if err != nil {
		return err
	}

	return c._store.Put(resourceName, func(r *logsv1.InitRecord, _ bool) {
		r.Logs = append(r.Logs, &logsv1.InitLog{
			Event:     logsv1.InitEvent_INIT_EVENT_FAILED,
			Context:   operator,
			Timestamp: time.Now().UnixNano(),
		})
	})
}

// Control acquisition status and persistence.
type acquireController struct {
	_kv        logs.ResourceRecordStore[logsv1.AcquisitionRecord]
	_resources sync.Map
}

func loadAcquireController(store logs.ResourceRecordStore[logsv1.AcquisitionRecord]) (*acquireController, error) {
	c := &acquireController{
		_kv: store,
	}

	err := store.ForEach(func(name string, obj *logsv1.AcquisitionRecord) error {
		acquired := map[string]int64{}
		// Replay stored acquisitions of the resource.
		for _, log := range obj.Logs {
			switch log.Event {
			case logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED:
				// Consecutive acquisition is not recorded.
				// So, we can skip the check of existing value.
				//
				// See: `(*acquireCtl).acquire()`
				acquired[logs.CallerContext(log.Context).String()] = log.N
			case logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED:
				// We assume that acquisition log is already processed.
				delete(acquired, logs.CallerContext(log.Context).String())
			}
		}
		// Set replayed acquireCtl.
		c._resources.Store(
			name,
			ctl.NewAcquisitionCtl(obj.Max, acquired),
		)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *acquireController) acquire(ctx context.Context, resourceName string, operator logs.CallerContext, max int64, exclusive bool) error {
	v, _ := c._resources.LoadOrStore(resourceName, ctl.NewAcquisitionCtl(max, map[string]int64{}))
	ctl := v.(*ctl.AcquisitionCtl)

	result := <-ctl.Acquire(ctx, operator.String(), exclusive)
	if result.Err != nil {
		return result.Err
	}
	if result.Acquired == 0 {
		// Due to trial of consecutive acquisition, not acquired actually.
		return nil
	}

	return c._kv.Put(resourceName, func(r *logsv1.AcquisitionRecord, update bool) {
		// Initial acquisition.
		if !update {
			r.Max = max
		}
		r.Logs = append(r.Logs, &logsv1.AcquisitionLog{
			Event:     logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
			N:         result.Acquired,
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
	ctl := v.(*ctl.AcquisitionCtl)
	if released := ctl.Release(operator.String()); !released {
		// If not acquired, return without error.
		return nil
	}

	return c._kv.Put(resourceName, func(r *logsv1.AcquisitionRecord, _ bool) {
		r.Logs = append(r.Logs, &logsv1.AcquisitionLog{
			Event:     logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED,
			N:         0,
			Context:   operator,
			Timestamp: time.Now().UnixNano(),
		})
	})
}
