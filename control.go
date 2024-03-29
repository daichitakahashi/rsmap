package rsmap

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/daichitakahashi/oncewait"
	"golang.org/x/sync/errgroup"

	"github.com/daichitakahashi/rsmap/internal/ctl"
	logsv1 "github.com/daichitakahashi/rsmap/internal/proto/logs/v1"
	resource_mapv1 "github.com/daichitakahashi/rsmap/internal/proto/resource_map/v1"
	"github.com/daichitakahashi/rsmap/internal/rendezvous"
	"github.com/daichitakahashi/rsmap/logs"
)

// TODO
// * timeout for init and acquisition

var errClosing = errors.New("closing")

type initController struct {
	_store     logs.ResourceRecordStore[logsv1.InitRecord]
	_resources sync.Map
	_closing   <-chan struct{}
}

func loadInitController(store logs.ResourceRecordStore[logsv1.InitRecord], closing <-chan struct{}) (*initController, error) {
	c := &initController{
		_store:   store,
		_closing: closing,
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
		initCtl := ctl.NewInitCtl(completed)
		if !completed {
			<-initCtl.TryInit(
				context.Background(),
				logs.CallerContext(last.Context).String(),
			)
		}
		c._resources.Store(name, initCtl)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *initController) tryInit(ctx context.Context, resourceName string, operator logs.CallerContext) (bool, error) {
	v, _ := c._resources.LoadOrStore(resourceName, ctl.NewInitCtl(false))
	initCtl := v.(*ctl.InitCtl)

	var result ctl.TryInitResult
	select {
	case <-c._closing:
		return false, errClosing
	case result = <-initCtl.TryInit(ctx, operator.String()):
		if result.Err != nil {
			return false, result.Err
		}
		if !result.Try {
			return false, nil
		}
	}

	if result.Initiated {
		// Update data on key value store.
		err := c._store.Put([]string{resourceName}, func(_ string, r *logsv1.InitRecord, _ bool) {
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
	select {
	case <-c._closing:
		return errClosing
	default:
	}

	v, found := c._resources.Load(resourceName)
	if !found {
		return errors.New("resource not found")
	}
	ctl := v.(*ctl.InitCtl)

	err := ctl.Complete(operator.String())
	if err != nil {
		return err
	}

	return c._store.Put([]string{resourceName}, func(_ string, r *logsv1.InitRecord, _ bool) {
		r.Logs = append(r.Logs, &logsv1.InitLog{
			Event:     logsv1.InitEvent_INIT_EVENT_COMPLETED,
			Context:   operator,
			Timestamp: time.Now().UnixNano(),
		})
	})
}

func (c *initController) fail(resourceName string, operator logs.CallerContext) error {
	select {
	case <-c._closing:
		return errClosing
	default:
	}

	v, found := c._resources.Load(resourceName)
	if !found {
		return errors.New("resource not found")
	}
	ctl := v.(*ctl.InitCtl)

	err := ctl.Fail(operator.String())
	if err != nil {
		return err
	}

	return c._store.Put([]string{resourceName}, func(_ string, r *logsv1.InitRecord, _ bool) {
		r.Logs = append(r.Logs, &logsv1.InitLog{
			Event:     logsv1.InitEvent_INIT_EVENT_FAILED,
			Context:   operator,
			Timestamp: time.Now().UnixNano(),
		})
	})
}

type (
	// Control acquisition status and persistence.
	acquireController struct {
		_kv        logs.ResourceRecordStore[logsv1.AcquisitionRecord]
		_resources sync.Map
		_closing   <-chan struct{}
		_multiMu   sync.Mutex
	}

	resource struct {
		once  *oncewait.OnceWaiter
		queue rendezvous.LimitedTermQueue
		ctl   *ctl.AcquisitionCtl
	}
)

func loadAcquireController(store logs.ResourceRecordStore[logsv1.AcquisitionRecord], acquiringQueueTimeout time.Duration, closing <-chan struct{}) (*acquireController, error) {
	c := &acquireController{
		_kv:      store,
		_closing: closing,
	}

	err := store.ForEach(func(name string, obj *logsv1.AcquisitionRecord) error {
		acquired := map[string]int64{}
		b := rendezvous.NewBuilder()
		// Replay stored acquisitions of the resource.
		for _, log := range obj.Logs {
			operator := logs.CallerContext(log.Context).String()
			switch log.Event {
			case logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRING:
				// Queue as "acquiring".
				b.Add(operator)
			case logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED:
				// Consecutive acquisition is not recorded.
				// So, we can skip the check of existing value.
				//
				// See: `(*ctl.AcquisitionCtl).Acquire()`
				acquired[operator] = log.N
				// Remove already acquired operation from queue.
				b.Remove(operator)
			case logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED:
				// We assume that acquisition log is already processed.
				delete(acquired, operator)
			}
		}
		// Set replayed acquireCtl.
		c._resources.Store(
			name,
			&resource{
				queue: b.Start(acquiringQueueTimeout),
				ctl:   ctl.NewAcquisitionCtl(obj.Max, acquired),
			},
		)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

var emptyQueue = rendezvous.NewBuilder().Start(0)

func (r *resource) init(max int64) *resource {
	if r.once != nil {
		r.once.Do(func() {
			r.queue = emptyQueue
			r.ctl = ctl.NewAcquisitionCtl(max, map[string]int64{})
		})
	}
	return r
}

func (r *resource) acquire(ctx context.Context, operator string, exclusive bool) (<-chan ctl.AcquisitionResult, bool) {
	var (
		ch        <-chan ctl.AcquisitionResult
		acquiring bool
	)

	// Wait dequeuing, because replayed "acquiring" operators take precedence.
	r.queue.Dequeue(operator, func(bool) {
		ch, acquiring = r.ctl.Acquire(ctx, operator, exclusive)
	})
	return ch, acquiring
}

func (c *acquireController) acquire(ctx context.Context, resourceName string, operator logs.CallerContext, max int64, exclusive bool) error {
	select {
	case <-c._closing:
		return errClosing
	default:
	}

	v, _ := c._resources.LoadOrStore(resourceName, &resource{
		once: oncewait.New(),
	})
	r := v.(*resource).init(max)

	// Start acquisition.
	acCh, acquiring := r.acquire(ctx, operator.String(), exclusive)
	// Due to trial of consecutive acquisition, not acquired.
	if !acquiring {
		return nil
	}

	// Append log "acquiring".
	err := c._kv.Put([]string{resourceName}, func(_ string, r *logsv1.AcquisitionRecord, update bool) {
		// Initial acquisition.
		if !update {
			r.Max = max
		}
		r.Logs = append(r.Logs, &logsv1.AcquisitionLog{
			Event:     logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRING,
			Context:   operator,
			Timestamp: time.Now().UnixNano(),
		})
	})
	if err != nil {
		return err
	}

	var result ctl.AcquisitionResult
	select {
	case <-c._closing:
		return errClosing
	case result = <-acCh:
		if result.Err != nil {
			return result.Err
		}
	}

	return c._kv.Put([]string{resourceName}, func(_ string, r *logsv1.AcquisitionRecord, update bool) {
		r.Logs = append(r.Logs, &logsv1.AcquisitionLog{
			Event:     logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
			N:         result.Acquired,
			Context:   operator,
			Timestamp: time.Now().UnixNano(),
		})
	})
}

func (c *acquireController) acquireMulti(ctx context.Context, resources []*resource_mapv1.AcquireMultiEntry) error {
	select {
	case <-c._closing:
		return errClosing
	default:
	}

	type acquiringEntry struct {
		entry    *resource_mapv1.AcquireMultiEntry
		acquired <-chan ctl.AcquisitionResult
	}
	identifiers := make([]string, 0, len(resources))
	entries := make(map[string]acquiringEntry, len(resources))

	// Lock for multiple locking.
	c._multiMu.Lock()
	for _, entry := range resources {
		v, _ := c._resources.LoadOrStore(entry.ResourceName, &resource{
			once: oncewait.New(),
		})
		r := v.(*resource).init(entry.MaxParallelism)

		// Start acquisition.
		acCh, acquiring := r.acquire(ctx, logs.CallerContext(entry.Context).String(), entry.Exclusive)
		// Due to trial of consecutive acquisition, not acquired.
		if acquiring {
			identifiers = append(identifiers, entry.ResourceName)
			entries[entry.ResourceName] = acquiringEntry{
				entry:    entry,
				acquired: acCh,
			}
		}
	}
	c._multiMu.Unlock()
	if len(identifiers) == 0 {
		return nil
	}

	// Append log "acquiring".
	ts := time.Now().UnixNano()
	err := c._kv.Put(identifiers, func(identifier string, r *logsv1.AcquisitionRecord, update bool) {
		e := entries[identifier]

		// Initial acquisition.
		if !update {
			r.Max = e.entry.MaxParallelism
		}
		r.Logs = append(r.Logs, &logsv1.AcquisitionLog{
			Event:     logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRING,
			Context:   e.entry.Context,
			Timestamp: ts,
		})
	})
	if err != nil {
		return err
	}

	eg, _ := errgroup.WithContext(ctx)

	for _, entry := range entries {
		e := entry
		eg.Go(func() error {
			var result ctl.AcquisitionResult
			select {
			case <-c._closing:
				return errClosing
			case result = <-e.acquired:
				if result.Err != nil {
					return err
				}
			}

			return c._kv.Put([]string{e.entry.ResourceName}, func(identifier string, r *logsv1.AcquisitionRecord, update bool) {
				r.Logs = append(r.Logs, &logsv1.AcquisitionLog{
					Event:     logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
					N:         result.Acquired,
					Context:   e.entry.Context,
					Timestamp: time.Now().UnixNano(),
				})
			})
		})
	}

	return eg.Wait()
}

func (c *acquireController) release(resourceName string, operator logs.CallerContext) error {
	select {
	case <-c._closing:
		return errClosing
	default:
	}

	v, found := c._resources.Load(resourceName)
	if !found {
		// If the resource not found, return without error.
		return nil
	}
	op := operator.String()
	r := v.(*resource)
	if !r.ctl.Acquired(op) {
		// If not acquired, return without error.
		return nil
	}

	err := c._kv.Put([]string{resourceName}, func(_ string, r *logsv1.AcquisitionRecord, _ bool) {
		r.Logs = append(r.Logs, &logsv1.AcquisitionLog{
			Event:     logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED,
			N:         0,
			Context:   operator,
			Timestamp: time.Now().UnixNano(),
		})
	})
	if err != nil {
		return err
	}

	r.ctl.Release(op)
	return nil
}

func (c *acquireController) releaseMulti(resources []*resource_mapv1.ReleaseMultiEntry) error {
	select {
	case <-c._closing:
		return errClosing
	default:
	}

	entries := make(map[string]*resource_mapv1.ReleaseMultiEntry, len(resources))
	identifiers := make([]string, 0, len(resources))

	for _, entry := range resources {
		v, found := c._resources.Load(entry.ResourceName)
		if !found {
			// If the resource not found, skip it.
			continue
		}
		r := v.(*resource)
		op := logs.CallerContext(entry.Context).String()
		if !r.ctl.Acquired(op) {
			// If not acquired, skip it.
			continue
		}
		entries[entry.ResourceName] = entry
		identifiers = append(identifiers, entry.ResourceName)

		// Release after log write.
		defer r.ctl.Release(op)
	}

	ts := time.Now().UnixNano()
	return c._kv.Put(identifiers, func(identifier string, r *logsv1.AcquisitionRecord, _ bool) {
		e := entries[identifier]

		r.Logs = append(r.Logs, &logsv1.AcquisitionLog{
			Event:     logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED,
			N:         0,
			Context:   e.Context,
			Timestamp: ts,
		})
	})
}
