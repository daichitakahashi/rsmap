package rsmap

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/rs/xid"
	"golang.org/x/sync/errgroup"
	"gotest.tools/v3/assert"
)

var background = context.Background()

var errDummyConnectionError = errors.New("dummy error")

type countTransport struct {
	recordedTimes []time.Time
}

func (t *countTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.recordedTimes = append(t.recordedTimes, time.Now())
	return nil, errDummyConnectionError
}

var _ http.RoundTripper = (*countTransport)(nil)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("rsmapDir exists as file", func(t *testing.T) {
		t.Parallel()

		// Create file.
		dir := filepath.Join(t.TempDir(), "file")
		assert.NilError(t, os.WriteFile(dir, []byte{'1'}, 0644))

		_, err := New(dir)
		var e *os.PathError
		assert.Assert(t, errors.As(err, &e))
		assert.DeepEqual(t, *e, os.PathError{
			Op:   "mkdir",
			Path: dir,
			Err:  syscall.ENOTDIR,
		})
	})

	t.Run("WithRetryPolicy and WithHTTPClient", func(t *testing.T) {
		base := t.TempDir()

		// Create dummy Map for preventing server mode.
		func() {
			m, err := New(base)
			assert.NilError(t, err)
			t.Cleanup(m.Close)

			// Confirm that the server is launched.
			_, err = m.Resource(background, "prepare")
			assert.NilError(t, err)
		}()

		var (
			p = backoff.NewConstantPolicy(
				backoff.WithInterval(time.Millisecond*100),
				backoff.WithMaxRetries(5),
			)
			tp countTransport
			c  = &http.Client{
				Transport: &tp,
				Timeout:   time.Millisecond * 100,
			}
		)

		m, err := New(base,
			WithRetryPolicy(p),
			WithHTTPClient(c),
		)
		assert.NilError(t, err)
		t.Cleanup(m.Close)

		_, err = m.Resource(background, "fails")
		assert.ErrorIs(t, err, errDummyConnectionError)
		assert.Assert(t, len(tp.recordedTimes) >= 5, len(tp.recordedTimes)) // First try and five retries will be recorded.
	})
}

func TestNew_FilesExistAsDirectory(t *testing.T) {
	t.Run("logs.db", func(t *testing.T) {
		t.Setenv(EnvExecutionID, "fixed")
		dir := t.TempDir()

		// Create logs.db as a directory.
		assert.NilError(t, os.MkdirAll(filepath.Join(dir, "fixed", "logs.db"), 0755))

		_, err := New(dir)
		assert.Assert(t, err != nil)
	})

	t.Run("addr", func(t *testing.T) {
		t.Setenv(EnvExecutionID, "fixed")
		dir := t.TempDir()

		// Create addr as a directory.
		assert.NilError(t, os.MkdirAll(filepath.Join(dir, "fixed", "addr"), 0755))

		_, err := New(dir)
		assert.Assert(t, err != nil)
	})
}

func TestMap_Resource(t *testing.T) {
	t.Parallel()

	t.Run("Use multiple resources by multiple Map", func(t *testing.T) {
		dir := t.TempDir()

		newMap := func(t *testing.T) *Map {
			t.Helper()

			m, err := New(dir)
			assert.NilError(t, err)
			t.Cleanup(m.Close)
			return m
		}

		treasure, err := newMap(t).Resource(background, "treasure")
		assert.NilError(t, err)
		assert.NilError(t, treasure.Lock(background))
		t.Cleanup(func() { _ = treasure.UnlockAny() })

		precious, err := newMap(t).Resource(background, "precious")
		assert.NilError(t, err)
		assert.NilError(t, precious.Lock(background))
		t.Cleanup(func() { _ = precious.UnlockAny() })
	})

	t.Run("MaxParallelism", func(t *testing.T) {
		t.Parallel()

		var (
			dir = t.TempDir()
			p   = WithMaxParallelism(3)
		)
		ctxWithTimeout := func(t *testing.T) context.Context {
			t.Helper()
			ctx, cancel := context.WithTimeout(background, time.Second)
			t.Cleanup(cancel)
			return ctx
		}
		newMapResource := func(t *testing.T) *Resource {
			t.Helper()

			m, err := New(dir)
			assert.NilError(t, err)
			t.Cleanup(m.Close)

			r, err := m.Resource(background, "trio", p)
			assert.NilError(t, err)
			return r
		}

		// Acquire shared locks until max parallelism(3).
		r1 := newMapResource(t)
		assert.NilError(t,
			r1.RLock(ctxWithTimeout(t)),
		)
		t.Cleanup(func() { _ = r1.UnlockAny() })

		r2 := newMapResource(t)
		assert.NilError(t,
			r2.RLock(ctxWithTimeout(t)),
		)
		t.Cleanup(func() { _ = r2.UnlockAny() })

		r3 := newMapResource(t)
		assert.NilError(t,
			r3.RLock(ctxWithTimeout(t)),
		)
		t.Cleanup(func() { _ = r3.UnlockAny() })

		// Try more acquisition. It will be timed out.
		r4 := newMapResource(t)
		assert.ErrorIs(t,
			r4.RLock(ctxWithTimeout(t)),
			context.DeadlineExceeded,
		)
	})

	t.Run("WithInit", func(t *testing.T) {
		t.Parallel()

		t.Run("Init succeeds only once", func(t *testing.T) {
			t.Parallel()

			var (
				dir   = t.TempDir()
				count int64
				i     = WithInit(func(ctx context.Context) error {
					atomic.AddInt64(&count, 1)
					return nil
				})
			)
			newResourceWithInit := func(t *testing.T) {
				t.Helper()

				m, err := New(dir)
				assert.NilError(t, err)
				t.Cleanup(m.Close)

				_, err = m.Resource(background, "init", i)
				assert.NilError(t, err)
			}

			// Try resource registration with init 5 times.
			newResourceWithInit(t)
			newResourceWithInit(t)
			newResourceWithInit(t)
			newResourceWithInit(t)
			newResourceWithInit(t)

			// Check count of init call.
			assert.Assert(t, count == 1)
		})

		t.Run("Init succeeds after the error", func(t *testing.T) {
			t.Parallel()

			var (
				dir        = t.TempDir()
				errFailure = errors.New("init failed")
				succeeded  bool
			)
			newMap := func(t *testing.T) *Map {
				t.Helper()

				m, err := New(dir)
				assert.NilError(t, err)
				t.Cleanup(m.Close)
				return m
			}

			// First try(server).
			_, err := newMap(t).Resource(background, "treasure", WithInit(func(ctx context.Context) error {
				return errFailure
			}))
			assert.ErrorIs(t, err, errFailure)

			// Second try(client).
			_, err = newMap(t).Resource(background, "treasure", WithInit(func(ctx context.Context) error {
				return errFailure
			}))
			assert.ErrorIs(t, err, errFailure)

			// Retry.
			_, err = newMap(t).Resource(background, "treasure", WithInit(func(ctx context.Context) error {
				succeeded = true
				return nil
			}))
			assert.NilError(t, err)
			assert.Assert(t, succeeded)
		})

		t.Run("Init succeeds after the panic", func(t *testing.T) {
			t.Parallel()

			var (
				dir       = t.TempDir()
				panicVal  = time.Now()
				succeeded bool
			)
			newMap := func(t *testing.T) *Map {
				t.Helper()

				m, err := New(dir)
				assert.NilError(t, err)
				t.Cleanup(m.Close)
				return m
			}

			// First try.
			recovered := func() (recovered any) {
				defer func() {
					recovered = recover()
				}()
				newMap(t).Resource(background, "treasure", WithInit(func(ctx context.Context) error {
					panic(panicVal)
				}))
				return nil
			}()
			assert.DeepEqual(t, recovered, panicVal)

			// Retry.
			_, err := newMap(t).Resource(background, "treasure", WithInit(func(ctx context.Context) error {
				succeeded = true
				return nil
			}))
			assert.NilError(t, err)
			assert.Assert(t, succeeded)
		})
	})
}

func TestLockResources_Deadlock(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	testFunc := func(fn func(ctx context.Context, treasure, precious *Resource)) func(context.Context) {
		ctx := context.Background()
		m, err := New(dir)
		assert.NilError(t, err)
		t.Cleanup(m.Close)

		treasure, err := m.Resource(ctx, "treasure")
		assert.NilError(t, err)
		precious, err := m.Resource(ctx, "precious")
		assert.NilError(t, err)

		return func(ctx context.Context) {
			fn(ctx, treasure, precious)
		}
	}

	stepOne := make(chan struct{})
	stepTwo := make(chan struct{})

	// Pseudo test function "TestA" and "TestB".
	// Both test uses "treasure" and "precious", but use Lock() in different order.
	// Ensure that this causes deadlock.
	TestA := testFunc(func(ctx context.Context, treasure, precious *Resource) {
		err := treasure.Lock(ctx)
		assert.NilError(t, err)

		close(stepOne)
		<-stepTwo

		err = precious.Lock(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	// TestB
	TestB := testFunc(func(ctx context.Context, treasure, precious *Resource) {
		<-stepOne

		err := precious.Lock(ctx)
		assert.NilError(t, err)

		close(stepTwo)

		err = treasure.Lock(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	// Run TestA and TestB.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		TestA(ctx)
	}()
	go func() {
		defer wg.Done()
		TestB(ctx)
	}()

	wg.Wait()
}

func TestLockResources(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	testFunc := func(fn func(ctx context.Context, treasure, precious *Resource) error) func(context.Context) error {
		ctx := context.Background()
		m, err := New(dir)
		assert.NilError(t, err)
		t.Cleanup(m.Close)

		treasure, err := m.Resource(ctx, "treasure")
		assert.NilError(t, err)
		precious, err := m.Resource(ctx, "precious")
		assert.NilError(t, err)

		return func(ctx context.Context) error {
			return fn(ctx, treasure, precious)
		}
	}

	// Resources as easy race detector.
	treasureData := map[string]bool{}
	preciousData := map[string]bool{}

	// Pseudo test function "TestA" and "TestB".
	// Both test uses "treasure" and "precious", but use Lock() in different order.
	// Ensure that deadlock never occurs.
	TestA := testFunc(func(ctx context.Context, treasure, precious *Resource) error {
		unlock, err := LockResources(ctx,
			treasure.Exclusive(),
			precious.Exclusive(),
		)
		if err != nil {
			return err
		}
		for i := 0; i < 5; i++ {
			time.Sleep(time.Millisecond)
			treasureData[xid.New().String()] = true
			preciousData[xid.New().String()] = true
		}
		return unlock()
	})

	// TestB
	TestB := testFunc(func(ctx context.Context, treasure, precious *Resource) error {
		unlock, err := LockResources(ctx,
			precious.Exclusive(),
			treasure.Exclusive(),
		)
		if err != nil {
			return err
		}
		for i := 0; i < 5; i++ {
			time.Sleep(time.Millisecond)
			preciousData[xid.New().String()] = true
			treasureData[xid.New().String()] = true
		}
		return unlock()
	})

	ctx, cancel := context.WithTimeout(background, time.Second*100)
	defer cancel()

	var eg errgroup.Group
	eg.Go(func() error {
		for i := 0; i < 20; i++ {
			err := TestA(ctx)
			if err != nil {
				return err
			}
		}
		return nil
	})
	eg.Go(func() error {
		for i := 0; i < 20; i++ {
			err := TestB(ctx)
			if err != nil {
				return err
			}
		}
		return nil
	})

	assert.NilError(t, eg.Wait())
}
