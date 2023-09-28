package ctl

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"gotest.tools/v3/assert"
)

var background = context.Background()

func TestSemaphore(t *testing.T) {
	t.Parallel()

	var (
		n   = runtime.GOMAXPROCS(0)
		sem = newSemaphore(int64(n))
		eg  errgroup.Group
	)

	for i := 0; i < n; i++ {
		i := i
		eg.Go(func() error {
			result := <-sem.acquire(background, int64(i), nil)
			if result.Err != nil {
				return result.Err
			}
			if result.Acquired != int64(i) {
				return fmt.Errorf("unexpected acquisition: expected=%d, actual=%d", i, result.Acquired)
			}
			sem.release(int64(i))
			return nil
		})
	}

	assert.NilError(t, eg.Wait())
}

func TestSemaphore_Panics(t *testing.T) {
	t.Parallel()

	t.Run("Try max-exceeded acquisition", func(t *testing.T) {
		t.Parallel()

		sem := newSemaphore(10)
		notPanicked := func() (notPanicked bool) {
			defer func() { recover() }()
			sem.acquire(background, 11, nil)
			notPanicked = true
			return
		}()

		assert.Assert(t, !notPanicked)
	})

	t.Run("Try release without acquisition", func(t *testing.T) {
		t.Parallel()

		sem := newSemaphore(10)
		notPanicked := func() (notPanicked bool) {
			defer func() { recover() }()
			sem.release(1)
			notPanicked = true
			return
		}()

		assert.Assert(t, !notPanicked)
	})
}

func TestSemaphore_Cancellation(t *testing.T) {
	t.Parallel()

	sem := newSemaphore(10)

	// Alice acquires.
	result := <-sem.acquire(background, 10, nil)
	assert.NilError(t, result.Err)
	assert.Assert(t, result.Acquired == 10)
	time.AfterFunc(time.Millisecond*500, func() {
		sem.release(10)
	})

	// Bob tries acquisition with timeout(200ms).
	started := time.Now()
	ctx, cancel := context.WithTimeout(background, time.Millisecond*200)
	defer cancel()
	result = <-sem.acquire(ctx, 1, nil)
	assert.ErrorIs(t, result.Err, context.DeadlineExceeded)
	assert.Assert(t, result.Acquired == 0)

	elapsed := time.Since(started)
	assert.Assert(t, time.Millisecond*200 < elapsed && elapsed < time.Millisecond*500)
}
