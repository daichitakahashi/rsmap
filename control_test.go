package rsmap

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
	"gotest.tools/v3/assert"

	logsv1 "github.com/daichitakahashi/rsmap/internal/proto/logs/v1"
	"github.com/daichitakahashi/rsmap/logs"
)

func asyncResult[T any](fn func() T) (result <-chan T) {
	ch := make(chan T)
	go func() {
		ch <- fn()
	}()
	return ch
}

var (
	callerAlice = logs.CallerContext{
		{
			File: "alice.go",
			Line: 10,
			Hash: "f1237f58",
		},
		{
			File: "alice.go",
			Line: 87,
			Hash: "9e9e9f3c",
		},
	}
	callerBob = logs.CallerContext{
		{
			File: "bob.go",
			Line: 8,
			Hash: "d3803c6e",
		},
		{
			File: "bob.go",
			Line: 144,
			Hash: "8695e950",
		},
	}
	callerCharlie = logs.CallerContext{
		{
			File: "charlie.go",
			Line: 88,
			Hash: "2c9c21db",
		},
		{
			File: "charlie.go",
			Line: 322,
			Hash: "349757b7",
		},
	}
)

var protoCmpOpts = []cmp.Option{
	cmpopts.IgnoreUnexported(
		logsv1.Caller{},
		logsv1.ServerRecord{},
		logsv1.ServerLog{},
		logsv1.InitRecord{},
		logsv1.InitLog{},
		logsv1.AcquisitionRecord{},
		logsv1.AcquisitionLog{},
	),
	cmpopts.IgnoreFields(logsv1.ServerLog{}, "Timestamp"),
	cmpopts.IgnoreFields(logsv1.InitLog{}, "Timestamp"),
	cmpopts.IgnoreFields(logsv1.AcquisitionLog{}, "Timestamp"),
}

func TestInitController(t *testing.T) {
	t.Parallel()

	t.Run("Init will be performed only once", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logsv1.InitRecord](db)
		assert.NilError(t, err)

		ctl, err := loadInitController(store)
		assert.NilError(t, err)

		// Start init by Alice.
		try, err := ctl.tryInit(background, "treasure", callerAlice)
		assert.NilError(t, err)
		assert.Assert(t, try)

		// Also, try to start init by Bob.
		type tryInitResult struct {
			try bool
			err error
		}
		bobsTry := asyncResult(func() tryInitResult {
			try, err := ctl.tryInit(background, "treasure", callerBob)
			return tryInitResult{
				try: try,
				err: err,
			}
		})

		// Pseudo init operation.
		time.Sleep(time.Millisecond * 100)

		// Check Bob's tryInit is not finished.
		select {
		case result := <-bobsTry:
			t.Fatalf("unexpected result of Bob's tryInit: it should be blocked: %+v", result)
		default:
			// Ok.
		}

		// Complete init by Alice.
		assert.NilError(t,
			ctl.complete("treasure", callerAlice),
		)

		// Check Bob's result again.
		// Init will be already completed.
		result := <-bobsTry
		assert.NilError(t, result.err)
		assert.Assert(t, !result.try)

		// Check stored logs.
		r, err := store.Get("treasure")
		assert.NilError(t, err)
		assert.DeepEqual(t, r, &logsv1.InitRecord{
			Logs: []*logsv1.InitLog{
				{
					Event:   logsv1.InitEvent_INIT_EVENT_STARTED,
					Context: callerAlice,
				}, {
					Event:   logsv1.InitEvent_INIT_EVENT_COMPLETED,
					Context: callerAlice,
				},
			},
		}, protoCmpOpts...)
	})

	t.Run("Consecutive try by same operator should be succeeded", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logsv1.InitRecord](db)
		assert.NilError(t, err)

		ctl, err := loadInitController(store)
		assert.NilError(t, err)

		// Start init by Alice.
		try, err := ctl.tryInit(background, "treasure", callerAlice)
		assert.NilError(t, err)
		assert.Assert(t, try)

		// Consecutive init.
		secondTry, err := ctl.tryInit(background, "treasure", callerAlice)
		assert.NilError(t, err)
		assert.Equal(t, try, secondTry)

		// Check stored logs.
		r, err := store.Get("treasure")
		assert.NilError(t, err)
		assert.DeepEqual(t, r, &logsv1.InitRecord{
			Logs: []*logsv1.InitLog{
				{
					Event:   logsv1.InitEvent_INIT_EVENT_STARTED,
					Context: callerAlice,
				},
			},
		}, protoCmpOpts...)
	})

	t.Run("Retry is allowed after the failure of first init", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logsv1.InitRecord](db)
		assert.NilError(t, err)

		ctl, err := loadInitController(store)
		assert.NilError(t, err)

		var (
			eg       errgroup.Group
			prepared = make(chan struct{})
			started  = make(chan struct{})
		)

		eg.Go(func() error {
			prepared <- struct{}{}
			<-started

			try, err := ctl.tryInit(background, "treasure", callerAlice)
			if err != nil {
				return err
			}
			if !try {
				return errors.New("try must be true")
			}

			return ctl.fail("treasure", callerAlice)
		})

		eg.Go(func() error {
			prepared <- struct{}{}
			<-started
			time.Sleep(time.Millisecond * 200)

			try, err := ctl.tryInit(background, "treasure", callerBob)
			if err != nil {
				return err
			}
			if !try {
				return errors.New("try must be true")
			}

			return nil
		})
		<-prepared
		<-prepared
		close(started)
		assert.NilError(t, eg.Wait())

		assert.NilError(t,
			ctl.complete("treasure", callerBob),
		)
	})

	t.Run("Replay the status that init is in progress", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logsv1.InitRecord](db)
		assert.NilError(t, err)

		// Setup situation that init has already started.
		assert.NilError(t,
			store.Put("treasure", func(r *logsv1.InitRecord, _ bool) {
				r.Logs = append(r.Logs, &logsv1.InitLog{
					Event:     logsv1.InitEvent_INIT_EVENT_STARTED,
					Context:   callerAlice,
					Timestamp: time.Now().UnixNano(),
				})
			}),
		)

		ctl, err := loadInitController(store)
		assert.NilError(t, err)

		// Bob's try, timed out.
		timedOut, cancel := context.WithDeadline(background, time.Now().Add(time.Millisecond))
		defer cancel()
		try, err := ctl.tryInit(timedOut, "treasure", callerBob)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Assert(t, !try)

		// Alice finishes init operation.
		assert.NilError(t, ctl.complete("treasure", callerAlice))

		// Bob receives completion of init.
		try, err = ctl.tryInit(background, "treasure", callerBob)
		assert.NilError(t, err)
		assert.Assert(t, !try)

		// Check stored logs.
		r, err := store.Get("treasure")
		assert.NilError(t, err)
		assert.DeepEqual(t, r, &logsv1.InitRecord{
			Logs: []*logsv1.InitLog{
				{
					Event:   logsv1.InitEvent_INIT_EVENT_STARTED,
					Context: callerAlice,
				}, {
					Event:   logsv1.InitEvent_INIT_EVENT_COMPLETED,
					Context: callerAlice,
				},
			},
		}, protoCmpOpts...)
	})

	t.Run("Replay the status that init is completed", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logsv1.InitRecord](db)
		assert.NilError(t, err)

		// Setup situation that init has already completed.
		assert.NilError(t,
			store.Put("treasure", func(r *logsv1.InitRecord, _ bool) {
				r.Logs = append(r.Logs, []*logsv1.InitLog{
					{
						Event:     logsv1.InitEvent_INIT_EVENT_STARTED,
						Context:   callerAlice,
						Timestamp: time.Now().UnixNano(),
					}, {
						Event:     logsv1.InitEvent_INIT_EVENT_COMPLETED,
						Context:   callerAlice,
						Timestamp: time.Now().UnixNano(),
					}}...)
			}),
		)

		ctl, err := loadInitController(store)
		assert.NilError(t, err)

		// Bob tries init, but already completed by Alice.
		try, err := ctl.tryInit(background, "treasure", callerBob)
		assert.NilError(t, err)
		assert.Assert(t, !try)
	})

	t.Run("Replay the status that init is failed", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logsv1.InitRecord](db)
		assert.NilError(t, err)

		ctl, err := loadInitController(store)
		assert.NilError(t, err)

		// Setup situation that init has failed.
		_, err = ctl.tryInit(background, "treasure", callerAlice)
		assert.NilError(t, err)
		assert.NilError(t, ctl.fail("treasure", callerAlice))

		replayed, err := loadInitController(store)
		assert.NilError(t, err)

		// Bob retries.
		try, err := replayed.tryInit(background, "treasure", callerBob)
		assert.NilError(t, err)
		assert.Assert(t, try)
		assert.NilError(t, replayed.complete("treasure", callerBob))
	})
}

func TestAcquireController(t *testing.T) {
	t.Parallel()

	t.Run("Lock works fine and logs are preserved correctly", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logsv1.AcquisitionRecord](db)
		assert.NilError(t, err)

		ctl, err := loadAcquireController(store)
		assert.NilError(t, err)

		// Acquire shared lock by Alice and Bob.
		assert.NilError(t,
			ctl.acquire(background, "treasure", callerAlice, 100, false),
		)
		assert.NilError(t,
			ctl.acquire(background, "treasure", callerBob, 100, false),
		)

		// Acquisition of exclusive lock by Charlie should be failed.
		timedOut, cancel := context.WithTimeout(background, time.Millisecond*100)
		defer cancel()
		assert.ErrorIs(t,
			ctl.acquire(timedOut, "treasure", callerCharlie, 100, true),
			context.DeadlineExceeded,
		)

		// Release shared locks.
		assert.NilError(t,
			ctl.release("treasure", callerAlice),
		)
		assert.NilError(t,
			ctl.release("treasure", callerBob),
		)

		// Retry of Charlie.
		assert.NilError(t,
			ctl.acquire(background, "treasure", callerCharlie, 100, true),
		)
		assert.NilError(t,
			ctl.release("treasure", callerCharlie),
		)

		// Check stored logs.
		r, err := store.Get("treasure")
		assert.NilError(t, err)
		assert.DeepEqual(t, r, &logsv1.AcquisitionRecord{
			Max: 100,
			Logs: []*logsv1.AcquisitionLog{
				{
					Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
					N:       1,
					Context: callerAlice,
				}, {
					Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
					N:       1,
					Context: callerBob,
				}, {
					Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED,
					Context: callerAlice,
				}, {
					Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED,
					Context: callerBob,
				}, {
					Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
					N:       100,
					Context: callerCharlie,
				}, {
					Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED,
					Context: callerCharlie,
				},
			},
		}, protoCmpOpts...)
	})

	t.Run("Consecutive acquire and release are succeeds but not recorded logs", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logsv1.AcquisitionRecord](db)
		assert.NilError(t, err)

		ctl, err := loadAcquireController(store)
		assert.NilError(t, err)

		// First acquisition.
		assert.NilError(t,
			ctl.acquire(background, "treasure", callerAlice, 100, true),
		)
		// Second acquisition without error(not acquired actually).
		assert.NilError(t,
			ctl.acquire(background, "treasure", callerAlice, 100, true),
		)

		// First release.
		assert.NilError(t,
			ctl.release("treasure", callerAlice),
		)
		// Second release without error(already released).
		assert.NilError(t,
			ctl.release("treasure", callerAlice),
		)

		// Check stored logs.
		r, err := store.Get("treasure")
		assert.NilError(t, err)
		assert.DeepEqual(t, r, &logsv1.AcquisitionRecord{
			Max: 100,
			Logs: []*logsv1.AcquisitionLog{
				{
					Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
					N:       100,
					Context: callerAlice,
				}, {
					Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED,
					N:       0,
					Context: callerAlice,
				},
			},
		}, protoCmpOpts...)
	})

	t.Run("Replay acquisition status correctly", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logsv1.AcquisitionRecord](db)
		assert.NilError(t, err)

		// Set up acquisition status.
		assert.NilError(t,
			store.Put("treasure", func(r *logsv1.AcquisitionRecord, _ bool) {
				r.Max = 10
				r.Logs = append(r.Logs, []*logsv1.AcquisitionLog{
					{
						Event:     logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
						N:         10,
						Context:   callerAlice,
						Timestamp: time.Now().UnixNano(),
					}, {
						Event:     logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED,
						Context:   callerAlice,
						Timestamp: time.Now().UnixNano(),
					}, {
						Event:     logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
						N:         1,
						Context:   callerAlice,
						Timestamp: 0,
					},
				}...)
			}),
		)
		assert.NilError(t,
			store.Put("precious", func(r *logsv1.AcquisitionRecord, _ bool) {
				r.Max = 200
				r.Logs = append(r.Logs, []*logsv1.AcquisitionLog{
					{
						Event:     logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
						N:         200,
						Context:   callerAlice,
						Timestamp: time.Now().UnixNano(),
					},
				}...)
			}),
		)

		ctl, err := loadAcquireController(store)
		assert.NilError(t, err)

		{
			// "treasure"

			// Alice's consecutive acquisition is ignored.
			assert.NilError(t,
				ctl.acquire(background, "treasure", callerAlice, 10, false),
			)

			// Bob's trial to acquire exclusive lock will be timed out.
			timedOut, cancel := context.WithTimeout(background, time.Millisecond*100)
			defer cancel()
			assert.ErrorIs(t,
				ctl.acquire(timedOut, "treasure", callerBob, 10, true),
				context.DeadlineExceeded,
			)
			// But shared lock can be acquired.
			assert.NilError(t,
				ctl.acquire(background, "treasure", callerBob, 10, false),
			)

			// Check stored logs.
			r, err := store.Get("treasure")
			assert.NilError(t, err)
			assert.DeepEqual(t, r, &logsv1.AcquisitionRecord{
				Max: 10,
				Logs: []*logsv1.AcquisitionLog{
					{
						Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
						N:       10,
						Context: callerAlice,
					}, {
						Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED,
						Context: callerAlice,
					}, {
						Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
						N:       1,
						Context: callerAlice,
					}, {
						Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
						N:       1,
						Context: callerBob,
					},
				},
			}, protoCmpOpts...)
		}

		{
			// "precious"

			// Acquisition of shared lock by Bob fails.
			timedOut, cancel := context.WithTimeout(background, time.Millisecond*100)
			defer cancel()
			assert.ErrorIs(t,
				ctl.acquire(timedOut, "precious", callerBob, 200, false),
				context.DeadlineExceeded,
			)

			// Release by Alice.
			assert.NilError(t,
				ctl.release("precious", callerAlice),
			)

			// Bob's acquisition succeeds now.
			assert.NilError(t,
				ctl.acquire(background, "precious", callerBob, 200, false),
			)

			// Check stored logs.
			r, err := store.Get("precious")
			assert.NilError(t, err)
			assert.DeepEqual(t, r, &logsv1.AcquisitionRecord{
				Max: 200,
				Logs: []*logsv1.AcquisitionLog{
					{
						Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
						N:       200,
						Context: callerAlice,
					}, {
						Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED,
						Context: callerAlice,
					}, {
						Event:   logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED,
						N:       1,
						Context: callerBob,
					},
				},
			}, protoCmpOpts...)
		}
	})
}
