package ctl_test

import (
	"context"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/daichitakahashi/rsmap/internal/ctl"
)

func TestAcquisitionCtl(t *testing.T) {
	t.Parallel()

	t.Run("exclusive lock", func(t *testing.T) {
		// Already, Alice has acquired shared lock.
		ctl := ctl.NewAcquisitionCtl(100, map[string]int64{
			"alice": 1,
		})

		// Bob's acquisition(exclusive) will be timed out.
		ctx, cancel := context.WithTimeout(background, time.Millisecond*100)
		defer cancel()
		result := <-ctl.Acquire(ctx, "bob", true)
		assert.ErrorIs(t, result.Err, context.DeadlineExceeded)
		assert.Equal(t, result.Acquired, int64(0))

		// Release by Alice.
		ctl.Release("alice")

		// Bob's acquisition(exclusive) will be succeeds.
		ctl.Acquire(background, "bob", true)
	})

	t.Run("shared lock", func(t *testing.T) {
		// Already, Alice and Bob has acquired shared lock.
		ctl := ctl.NewAcquisitionCtl(2, map[string]int64{
			"alice": 1,
			"bob":   1,
		})

		// Charlie's acquisition(shared) will be timed out.
		ctx, cancel := context.WithTimeout(background, time.Millisecond*100)
		defer cancel()
		result := <-ctl.Acquire(ctx, "charlie", false)
		assert.ErrorIs(t, result.Err, context.DeadlineExceeded)
		assert.Equal(t, result.Acquired, int64(0))

		// Release by Alice(Bob's acquisition is continued).
		ctl.Release("alice")

		// Charlie's acquisition(shared) will be succeeds.
		ctl.Acquire(background, "charlie", false)
	})

	t.Run("consecutive acquisition without release returns nil error", func(t *testing.T) {
		// Create fresh one.
		ctl := ctl.NewAcquisitionCtl(100, map[string]int64{})

		// First acquisition.
		result := <-ctl.Acquire(background, "alice", true)
		assert.NilError(t, result.Err)
		assert.Equal(t, result.Acquired, int64(100))

		// Second acquisition without release.
		// Acquired weight is 0.
		result = <-ctl.Acquire(background, "alice", true)
		assert.NilError(t, result.Err)
		assert.Equal(t, result.Acquired, int64(0))
	})

	t.Run("unknown release returns nil error", func(t *testing.T) {
		// Create fresh one.
		ctl := ctl.NewAcquisitionCtl(100, map[string]int64{})

		// Unknown acquisition will be succeeded, without panic or something.
		ctl.Release("alice")
	})
}
