package rsmap

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"
	"go.etcd.io/bbolt"
	"gotest.tools/v3/assert"

	"github.com/daichitakahashi/rsmap/logs"
)

func asyncResult[T any](fn func() T) (result <-chan T) {
	ch := make(chan T)
	go func() {
		ch <- fn()
	}()
	return ch
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

		store, err := logs.NewResourceRecordStore[logs.InitRecord](db)
		assert.NilError(t, err)

		ctl, err := loadInitController(store)
		assert.NilError(t, err)

		// Start init by Alice.
		try, err := ctl.tryInit(background, "treasure", "alice")
		assert.NilError(t, err)
		assert.Assert(t, try)

		// Also, try to start init by Bob.
		type tryInitResult struct {
			try bool
			err error
		}
		bobsTry := asyncResult(func() tryInitResult {
			try, err := ctl.tryInit(background, "treasure", "bob")
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
			ctl.complete("treasure", "alice"),
		)

		// Check Bob's result again.
		// Init will be already completed.
		result := <-bobsTry
		assert.NilError(t, result.err)
		assert.Assert(t, !result.try)

		// Check stored logs.
		r, err := store.Get("treasure")
		assert.NilError(t, err)
		assert.DeepEqual(t, *r, logs.InitRecord{
			Logs: []logs.InitLog{
				{
					Event:    logs.InitEventStarted,
					Operator: "alice",
				}, {
					Event:    logs.InitEventCompleted,
					Operator: "alice",
				},
			},
		}, cmpopts.IgnoreFields(logs.InitLog{}, "Timestamp"))
	})

	t.Run("Consecutive try by same operator should be succeeded", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logs.InitRecord](db)
		assert.NilError(t, err)

		ctl, err := loadInitController(store)
		assert.NilError(t, err)

		// Start init by Alice.
		try, err := ctl.tryInit(background, "treasure", "alice")
		assert.NilError(t, err)
		assert.Assert(t, try)

		// Consecutive init.
		secondTry, err := ctl.tryInit(background, "treasure", "alice")
		assert.NilError(t, err)
		assert.Equal(t, try, secondTry)

		// Check stored logs.
		r, err := store.Get("treasure")
		assert.NilError(t, err)
		assert.DeepEqual(t, *r, logs.InitRecord{
			Logs: []logs.InitLog{
				{
					Event:    logs.InitEventStarted,
					Operator: "alice",
				},
			},
		}, cmpopts.IgnoreFields(logs.InitLog{}, "Timestamp"))
	})

	t.Run("Replay the status that init is in progress", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logs.InitRecord](db)
		assert.NilError(t, err)

		// Setup situation that init has already started.
		assert.NilError(t,
			store.Put("treasure", func(r *logs.InitRecord, _ bool) {
				r.Logs = append(r.Logs, logs.InitLog{
					Event:     logs.InitEventStarted,
					Operator:  "alice",
					Timestamp: time.Now().UnixNano(),
				})
			}),
		)

		ctl, err := loadInitController(store)
		assert.NilError(t, err)

		// Bob's try, timed out.
		timedOut, cancel := context.WithDeadline(background, time.Now().Add(time.Millisecond))
		defer cancel()
		try, err := ctl.tryInit(timedOut, "treasure", "bob")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Assert(t, !try)

		// Alice finishes init operation.
		assert.NilError(t, ctl.complete("treasure", "alice"))

		// Bob receives completion of init.
		try, err = ctl.tryInit(background, "treasure", "bob")
		assert.NilError(t, err)
		assert.Assert(t, !try)

		// Check stored logs.
		r, err := store.Get("treasure")
		assert.NilError(t, err)
		assert.DeepEqual(t, *r, logs.InitRecord{
			Logs: []logs.InitLog{
				{
					Event:    logs.InitEventStarted,
					Operator: "alice",
				}, {
					Event:    logs.InitEventCompleted,
					Operator: "alice",
				},
			},
		}, cmpopts.IgnoreFields(logs.InitLog{}, "Timestamp"))
	})

	t.Run("Replay the status that init is completed", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logs.InitRecord](db)
		assert.NilError(t, err)

		// Setup situation that init has already completed.
		assert.NilError(t,
			store.Put("treasure", func(r *logs.InitRecord, _ bool) {
				r.Logs = append(r.Logs, []logs.InitLog{
					{
						Event:     logs.InitEventCompleted,
						Operator:  "alice",
						Timestamp: time.Now().UnixNano(),
					}, {
						Event:     logs.InitEventCompleted,
						Operator:  "alice",
						Timestamp: time.Now().UnixNano(),
					}}...)
			}),
		)

		ctl, err := loadInitController(store)
		assert.NilError(t, err)

		// Bob tries init, but already completed by Alice.
		try, err := ctl.tryInit(background, "treasure", "bob")
		assert.NilError(t, err)
		assert.Assert(t, !try)
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

		store, err := logs.NewResourceRecordStore[logs.AcquireRecord](db)
		assert.NilError(t, err)

		ctl, err := loadAcquireController(store)
		assert.NilError(t, err)

		// Acquire shared lock by Alice and Bob.
		assert.NilError(t,
			ctl.acquire(background, "treasure", "alice", 100, false),
		)
		assert.NilError(t,
			ctl.acquire(background, "treasure", "bob", 100, false),
		)

		// Acquisition of exclusive lock by Charlie should be failed.
		timedOut, cancel := context.WithTimeout(background, time.Millisecond*100)
		defer cancel()
		assert.ErrorIs(t,
			ctl.acquire(timedOut, "treasure", "charlie", 100, true),
			context.DeadlineExceeded,
		)

		// Release shared locks.
		assert.NilError(t,
			ctl.release("treasure", "alice"),
		)
		assert.NilError(t,
			ctl.release("treasure", "bob"),
		)

		// Retry of Charlie.
		assert.NilError(t,
			ctl.acquire(background, "treasure", "charlie", 100, true),
		)
		assert.NilError(t,
			ctl.release("treasure", "charlie"),
		)

		// Check stored logs.
		r, err := store.Get("treasure")
		assert.NilError(t, err)
		assert.DeepEqual(t, *r, logs.AcquireRecord{
			Max: 100,
			Logs: []logs.AcquireLog{
				{
					Event:    logs.AcquireEventAcquired,
					N:        1,
					Operator: "alice",
				}, {
					Event:    logs.AcquireEventAcquired,
					N:        1,
					Operator: "bob",
				}, {
					Event:    logs.AcquireEventReleased,
					Operator: "alice",
				}, {
					Event:    logs.AcquireEventReleased,
					Operator: "bob",
				}, {
					Event:    logs.AcquireEventAcquired,
					N:        100,
					Operator: "charlie",
				}, {
					Event:    logs.AcquireEventReleased,
					Operator: "charlie",
				},
			},
		}, cmpopts.IgnoreFields(logs.AcquireLog{}, "Timestamp"))
	})

	t.Run("Consecutive acquire and release are succeeds but not recorded logs", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logs.AcquireRecord](db)
		assert.NilError(t, err)

		ctl, err := loadAcquireController(store)
		assert.NilError(t, err)

		// First acquisition.
		assert.NilError(t,
			ctl.acquire(background, "treasure", "alice", 100, true),
		)
		// Second acquisition without error(not acquired actually).
		assert.NilError(t,
			ctl.acquire(background, "treasure", "alice", 100, true),
		)

		// First release.
		assert.NilError(t,
			ctl.release("treasure", "alice"),
		)
		// Second release without error(already released).
		assert.NilError(t,
			ctl.release("treasure", "alice"),
		)

		// Check stored logs.
		r, err := store.Get("treasure")
		assert.NilError(t, err)
		assert.DeepEqual(t, *r, logs.AcquireRecord{
			Max: 100,
			Logs: []logs.AcquireLog{
				{
					Event:    logs.AcquireEventAcquired,
					N:        100,
					Operator: "alice",
				}, {
					Event:    logs.AcquireEventReleased,
					N:        0,
					Operator: "alice",
				},
			},
		}, cmpopts.IgnoreFields(logs.AcquireLog{}, "Timestamp"))
	})

	t.Run("Replay acquisition status correctly", func(t *testing.T) {
		t.Parallel()

		db, err := bbolt.Open(filepath.Join(t.TempDir(), "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		store, err := logs.NewResourceRecordStore[logs.AcquireRecord](db)
		assert.NilError(t, err)

		// Set up acquisition status.
		assert.NilError(t,
			store.Put("treasure", func(r *logs.AcquireRecord, _ bool) {
				r.Max = 10
				r.Logs = append(r.Logs, []logs.AcquireLog{
					{
						Event:     logs.AcquireEventAcquired,
						N:         10,
						Operator:  "alice",
						Timestamp: time.Now().UnixNano(),
					}, {
						Event:     logs.AcquireEventReleased,
						Operator:  "alice",
						Timestamp: time.Now().UnixNano(),
					}, {
						Event:     logs.AcquireEventAcquired,
						N:         1,
						Operator:  "alice",
						Timestamp: 0,
					},
				}...)
			}),
		)
		assert.NilError(t,
			store.Put("precious", func(r *logs.AcquireRecord, _ bool) {
				r.Max = 200
				r.Logs = append(r.Logs, []logs.AcquireLog{
					{
						Event:     logs.AcquireEventAcquired,
						N:         200,
						Operator:  "alice",
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
				ctl.acquire(background, "treasure", "alice", 10, false),
			)

			// Bob's trial to acquire exclusive lock will be timed out.
			timedOut, cancel := context.WithTimeout(background, time.Millisecond*100)
			defer cancel()
			assert.ErrorIs(t,
				ctl.acquire(timedOut, "treasure", "bob", 10, true),
				context.DeadlineExceeded,
			)
			// But shared lock can be acquired.
			assert.NilError(t,
				ctl.acquire(background, "treasure", "bob", 10, false),
			)

			// Check stored logs.
			r, err := store.Get("treasure")
			assert.NilError(t, err)
			assert.DeepEqual(t, *r, logs.AcquireRecord{
				Max: 10,
				Logs: []logs.AcquireLog{
					{
						Event:    logs.AcquireEventAcquired,
						N:        10,
						Operator: "alice",
					}, {
						Event:    logs.AcquireEventReleased,
						Operator: "alice",
					}, {
						Event:    logs.AcquireEventAcquired,
						N:        1,
						Operator: "alice",
					}, {
						Event:    logs.AcquireEventAcquired,
						N:        1,
						Operator: "bob",
					},
				},
			}, cmpopts.IgnoreFields(logs.AcquireLog{}, "Timestamp"))
		}

		{
			// "precious"

			// Acquisition of shared lock by Bob fails.
			timedOut, cancel := context.WithTimeout(background, time.Millisecond*100)
			defer cancel()
			assert.ErrorIs(t,
				ctl.acquire(timedOut, "precious", "bob", 200, false),
				context.DeadlineExceeded,
			)

			// Release by Alice.
			assert.NilError(t,
				ctl.release("precious", "alice"),
			)

			// Bob's acquisition succeeds now.
			assert.NilError(t,
				ctl.acquire(background, "precious", "bob", 200, false),
			)

			// Check stored logs.
			r, err := store.Get("precious")
			assert.NilError(t, err)
			assert.DeepEqual(t, *r, logs.AcquireRecord{
				Max: 200,
				Logs: []logs.AcquireLog{
					{
						Event:    logs.AcquireEventAcquired,
						N:        200,
						Operator: "alice",
					}, {
						Event:    logs.AcquireEventReleased,
						Operator: "alice",
					}, {
						Event:    logs.AcquireEventAcquired,
						N:        1,
						Operator: "bob",
					},
				},
			}, cmpopts.IgnoreFields(logs.AcquireLog{}, "Timestamp"))
		}
	})
}
