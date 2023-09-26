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
		acCh, acquiring := ctl.Acquire(ctx, "bob", true)
		assert.Assert(t, acquiring)

		result := <-acCh
		assert.ErrorIs(t, result.Err, context.DeadlineExceeded)
		assert.Assert(t, result.Acquired == 0)

		// Release by Alice.
		ctl.Release("alice")

		// Bob's acquisition(exclusive) will be succeed.
		acCh, acquiring = ctl.Acquire(background, "bob", true)
		assert.Assert(t, acquiring)
		result = <-acCh
		assert.NilError(t, result.Err)
		assert.Assert(t, result.Acquired == 100)
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
		acCh, acquiring := ctl.Acquire(ctx, "charlie", false)
		assert.Assert(t, acquiring)

		result := <-acCh
		assert.ErrorIs(t, result.Err, context.DeadlineExceeded)
		assert.Assert(t, result.Acquired == 0)

		// Release by Alice(Bob's acquisition is continued).
		ctl.Release("alice")

		// Charlie's acquisition(shared) will be succeeds.
		acCh, acquiring = ctl.Acquire(background, "charlie", false)
		assert.Assert(t, acquiring)

		result = <-acCh
		assert.NilError(t, result.Err)
		assert.Assert(t, result.Acquired == 1)
	})

	t.Run("consecutive acquisition without release returns nil error", func(t *testing.T) {
		// Create fresh one.
		ctl := ctl.NewAcquisitionCtl(100, map[string]int64{})

		// First acquisition.
		acCh, acquiring := ctl.Acquire(background, "alice", true)
		assert.Assert(t, acquiring)

		result := <-acCh
		assert.NilError(t, result.Err)
		assert.Assert(t, result.Acquired == 100)

		// Second acquisition without release.
		// Acquired weight is 0.
		acCh, acquiring = ctl.Acquire(background, "alice", true)
		assert.Assert(t, !acquiring)
		assert.Assert(t, acCh == nil)
	})

	t.Run("unknown release returns nil error", func(t *testing.T) {
		// Create fresh one.
		ctl := ctl.NewAcquisitionCtl(100, map[string]int64{})

		// Unknown acquisition will be succeeded, without panic or something.
		ctl.Release("alice")
	})
}
