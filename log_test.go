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

		ctl := loadInitController(kv)

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

		ctl := loadInitController(kv)

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
		kv.EXPECT().forEach(gomock.Any()).DoAndReturn(func(f func(string, *initRecord) error) error {
			f("treasure", &r)
			return nil
		}).Times(1)
		kv.EXPECT().get("treasure").Return(&r, nil).Times(1)
		kv.EXPECT().set("treasure", &r).Return(nil).Times(1)

		ctl := loadInitController(kv)

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
		kv.EXPECT().forEach(gomock.Any()).DoAndReturn(func(f func(string, *initRecord) error) error {
			f("treasure", &r)
			return nil
		}).Times(1)

		ctl := loadInitController(kv)

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
}
