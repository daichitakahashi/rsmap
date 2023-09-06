package rsmap

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"gotest.tools/v3/assert"
)

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
