package rsmap

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"
	"gotest.tools/v3/assert"
)

var background = context.Background()

func TestInitCtl(t *testing.T) {
	t.Parallel()

	var (
		ctl    = newInitCtl(false)
		begin  = make(chan struct{})
		eg     errgroup.Group
		result = bytes.NewBuffer(nil)
	)

	for i := 0; i < 10; i++ {
		operator := strconv.Itoa(i)
		eg.Go(func() error {
			<-begin

			var started bool
			err := ctl.tryInit(background, operator, func(try bool) error {
				if try {
					time.Sleep(time.Second)
					fmt.Fprintln(result, "Start initialization!")
					started = true
					return nil
				} else {
					fmt.Fprintln(result, "Already initialized.")
				}
				return nil
			})
			if err != nil {
				return err
			}

			if started {
				return ctl.complete(operator, func() error {
					fmt.Fprintln(result, "Completed.")
					return nil
				})
			}
			return nil
		})
	}

	close(begin)
	assert.NilError(t, eg.Wait())
	assert.DeepEqual(t, result.String(), `Start initialization!
Completed.
Already initialized.
Already initialized.
Already initialized.
Already initialized.
Already initialized.
Already initialized.
Already initialized.
Already initialized.
Already initialized.
`)
}

func TestInitController(t *testing.T) {
	t.Parallel()

	t.Run("Init will be performed only once", func(t *testing.T) {
		t.Parallel()

		var (
			kv = NewMockkeyValueStore[initRecord](
				gomock.NewController(t),
			)
			r initRecord
		)
		kv.EXPECT().forEach(gomock.Any()).
			Return(nil).
			Times(1)
		kv.EXPECT().get("treasure").
			Return(&r, nil).
			Times(2) // Called by tryInit and complete.
		kv.EXPECT().set("treasure", &r).
			Return(nil).
			Times(2) // Called by tryInit and complete.

		ctl, err := loadInitController(kv)
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
		assert.DeepEqual(t, r, initRecord{
			Logs: []initLog{
				{
					Event:    initEventStarted,
					Operator: "alice",
				}, {
					Event:    initEventCompleted,
					Operator: "alice",
				},
			},
		}, cmpopts.IgnoreFields(initLog{}, "Timestamp"))
	})

	t.Run("Consecutive try by same operator should be succeeded", func(t *testing.T) {
		t.Parallel()

		var (
			kv = NewMockkeyValueStore[initRecord](
				gomock.NewController(t),
			)
			r initRecord
		)
		kv.EXPECT().forEach(gomock.Any()).
			Return(nil).
			Times(1)
		kv.EXPECT().get("treasure").
			Return(&r, nil).
			Times(1)
		kv.EXPECT().set("treasure", &r).
			Return(nil).
			Times(1)

		ctl, err := loadInitController(kv)
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
		assert.DeepEqual(t, r, initRecord{
			Logs: []initLog{
				{
					Event:    initEventStarted,
					Operator: "alice",
				},
			},
		}, cmpopts.IgnoreFields(initLog{}, "Timestamp"))
	})

	t.Run("Replay the status that init is in progress", func(t *testing.T) {
		t.Parallel()

		var (
			kv = NewMockkeyValueStore[initRecord](
				gomock.NewController(t),
			)
			r = initRecord{
				Logs: []initLog{
					{
						Event:     initEventStarted,
						Operator:  "alice",
						Timestamp: time.Now().UnixNano(),
					},
				},
			}
		)
		// Restore init try by Alice.
		kv.EXPECT().forEach(gomock.Any()).
			DoAndReturn(func(f func(string, *initRecord) error) error {
				f("treasure", &r)
				return nil
			}).Times(1)
		kv.EXPECT().get("treasure").Return(&r, nil).Times(1)
		kv.EXPECT().set("treasure", &r).Return(nil).Times(1)

		ctl, err := loadInitController(kv)
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
		assert.DeepEqual(t, r, initRecord{
			Logs: []initLog{
				{
					Event:    initEventStarted,
					Operator: "alice",
				}, {
					Event:    initEventCompleted,
					Operator: "alice",
				},
			},
		}, cmpopts.IgnoreFields(initLog{}, "Timestamp"))
	})

	t.Run("Replay the status that init is completed", func(t *testing.T) {
		t.Parallel()

		var (
			kv = NewMockkeyValueStore[initRecord](
				gomock.NewController(t),
			)
			r = initRecord{
				Logs: []initLog{
					{
						Event:     initEventCompleted,
						Operator:  "alice",
						Timestamp: time.Now().UnixNano(),
					}, {
						Event:     initEventCompleted,
						Operator:  "alice",
						Timestamp: time.Now().UnixNano(),
					},
				},
			}
		)
		kv.EXPECT().forEach(gomock.Any()).
			DoAndReturn(func(f func(string, *initRecord) error) error {
				f("treasure", &r)
				return nil
			}).Times(1)

		ctl, err := loadInitController(kv)
		assert.NilError(t, err)

		// Bob tries init, but already completed by Alice.
		try, err := ctl.tryInit(background, "treasure", "bob")
		assert.NilError(t, err)
		assert.Assert(t, !try)
	})
}

func TestAcquireCtl(t *testing.T) {
	t.Parallel()

	t.Run("exclusive lock", func(t *testing.T) {
		// Already, Alice has acquired shared lock.
		ctl := newAcquireCtl(100, map[string]int64{
			"alice": 1,
		})

		// Bob's acquisition(exclusive) will be timed out.
		ctx, cancel := context.WithTimeout(background, time.Millisecond*100)
		defer cancel()
		n, err := ctl.acquire(ctx, "bob", true)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, n, int64(0))

		// Release by Alice.
		ctl.release("alice")

		// Bob's acquisition(exclusive) will be succeeds.
		ctl.acquire(background, "bob", true)
	})

	t.Run("shared lock", func(t *testing.T) {
		// Already, Alice and Bob has acquired shared lock.
		ctl := newAcquireCtl(2, map[string]int64{
			"alice": 1,
			"bob":   1,
		})

		// Charlie's acquisition(shared) will be timed out.
		ctx, cancel := context.WithTimeout(background, time.Millisecond*100)
		defer cancel()
		n, err := ctl.acquire(ctx, "charlie", false)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, n, int64(0))

		// Release by Alice(Bob's acquisition is continued).
		ctl.release("alice")

		// Charlie's acquisition(shared) will be succeeds.
		ctl.acquire(background, "charlie", false)
	})

	t.Run("consecutive acquisition without release returns nil error", func(t *testing.T) {
		// Create fresh one.
		ctl := newAcquireCtl(100, map[string]int64{})

		// First acquisition.
		n, err := ctl.acquire(background, "alice", true)
		assert.NilError(t, err)
		assert.Equal(t, n, int64(100))

		// Second acquisition without release.
		// Acquired weight is 0.
		n, err = ctl.acquire(background, "alice", true)
		assert.NilError(t, err)
		assert.Equal(t, n, int64(0))
	})

	t.Run("unknown release returns nil error", func(t *testing.T) {
		// Create fresh one.
		ctl := newAcquireCtl(100, map[string]int64{})

		// Unknown acquisition will be succeeded, without panic or something.
		ctl.release("alice")
	})
}

func TestAcquireController(t *testing.T) {
	t.Parallel()

	t.Run("Lock works fine and logs are preserved correctly", func(t *testing.T) {
		t.Parallel()

		var (
			kv = NewMockkeyValueStore[acquireRecord](
				gomock.NewController(t),
			)
			r *acquireRecord
		)
		kv.EXPECT().forEach(gomock.Any()).
			Return(nil).
			Times(1)
		kv.EXPECT().get("treasure").
			DoAndReturn(func(string) (*acquireRecord, error) {
				if r == nil {
					return nil, errRecordNotFound
				}
				return r, nil
			}).Times(6)
		kv.EXPECT().set("treasure", gomock.Any()).
			DoAndReturn(func(_ string, set *acquireRecord) error {
				r = set
				return nil
			}).Times(6)

		ctl, err := loadAcquireController(kv)
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
		assert.DeepEqual(t, *r, acquireRecord{
			Max: 100,
			Logs: []acquireLog{
				{
					Event:    acquireEventAcquired,
					N:        1,
					Operator: "alice",
				}, {
					Event:    acquireEventAcquired,
					N:        1,
					Operator: "bob",
				}, {
					Event:    acquireEventReleased,
					Operator: "alice",
				}, {
					Event:    acquireEventReleased,
					Operator: "bob",
				}, {
					Event:    acquireEventAcquired,
					N:        100,
					Operator: "charlie",
				}, {
					Event:    acquireEventReleased,
					Operator: "charlie",
				},
			},
		}, cmpopts.IgnoreFields(acquireLog{}, "Timestamp"))
	})

	t.Run("Consecutive acquire and release are succeeds but not recorded logs", func(t *testing.T) {
		t.Parallel()

		var (
			kv = NewMockkeyValueStore[acquireRecord](
				gomock.NewController(t),
			)
			r *acquireRecord
		)
		kv.EXPECT().forEach(gomock.Any()).
			Return(nil).
			Times(1)
		kv.EXPECT().get("treasure").
			DoAndReturn(func(s string) (*acquireRecord, error) {
				if r == nil {
					return nil, errRecordNotFound
				}
				return r, nil
			}).Times(2) // Called by acquire and release.
		kv.EXPECT().set("treasure", gomock.Any()).
			DoAndReturn(func(s string, set *acquireRecord) error {
				r = set
				return nil
			}).Times(2)

		ctl, err := loadAcquireController(kv)
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
		assert.DeepEqual(t, *r, acquireRecord{
			Max: 100,
			Logs: []acquireLog{
				{
					Event:    acquireEventAcquired,
					N:        100,
					Operator: "alice",
				}, {
					Event:    acquireEventReleased,
					N:        0,
					Operator: "alice",
				},
			},
		}, cmpopts.IgnoreFields(acquireLog{}, "Timestamp"))
	})

	t.Run("Replay acquisition status correctly", func(t *testing.T) {
		t.Parallel()

		var (
			mc             = gomock.NewController(t)
			kv             = NewMockkeyValueStore[acquireRecord](mc)
			treasureRecord = &acquireRecord{
				Max: 10,
				Logs: []acquireLog{
					{
						Event:     acquireEventAcquired,
						N:         10,
						Operator:  "alice",
						Timestamp: time.Now().UnixNano(),
					}, {
						Event:     acquireEventReleased,
						Operator:  "alice",
						Timestamp: time.Now().UnixNano(),
					}, {
						Event:     acquireEventAcquired,
						N:         1,
						Operator:  "alice",
						Timestamp: 0,
					},
				},
			}
			preciousRecord = &acquireRecord{
				Max: 200,
				Logs: []acquireLog{
					{
						Event:     acquireEventAcquired,
						N:         200,
						Operator:  "alice",
						Timestamp: time.Now().UnixNano(),
					},
				},
			}
		)
		// Replay stored operations.
		kv.EXPECT().forEach(gomock.Any()).
			DoAndReturn(func(f func(string, *acquireRecord) error) error {
				records := map[string]*acquireRecord{
					"treasure": treasureRecord,
					"precious": preciousRecord,
				}
				for name, record := range records {
					err := f(name, record)
					assert.NilError(t, err)
				}
				return nil
			})

		ctl, err := loadAcquireController(kv)
		assert.NilError(t, err)

		{
			// "treasure"

			kv.EXPECT().get("treasure").
				Return(treasureRecord, nil).
				Times(1)
			kv.EXPECT().set("treasure", treasureRecord).
				Times(1)

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
			assert.DeepEqual(t, *treasureRecord, acquireRecord{
				Max: 10,
				Logs: []acquireLog{
					{
						Event:    acquireEventAcquired,
						N:        10,
						Operator: "alice",
					}, {
						Event:    acquireEventReleased,
						Operator: "alice",
					}, {
						Event:    acquireEventAcquired,
						N:        1,
						Operator: "alice",
					}, {
						Event:    acquireEventAcquired,
						N:        1,
						Operator: "bob",
					},
				},
			}, cmpopts.IgnoreFields(acquireLog{}, "Timestamp"))
		}

		{
			// "precious"

			kv.EXPECT().get("precious").
				Return(preciousRecord, nil).
				Times(2)
			kv.EXPECT().set("precious", preciousRecord).
				Times(2)

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
			assert.DeepEqual(t, *preciousRecord, acquireRecord{
				Max: 200,
				Logs: []acquireLog{
					{
						Event:    acquireEventAcquired,
						N:        200,
						Operator: "alice",
					}, {
						Event:    acquireEventReleased,
						Operator: "alice",
					}, {
						Event:    acquireEventAcquired,
						N:        1,
						Operator: "bob",
					},
				},
			}, cmpopts.IgnoreFields(acquireLog{}, "Timestamp"))
		}
	})
}
