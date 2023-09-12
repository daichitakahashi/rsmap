package rsmap

import (
	"context"
	"errors"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"go.etcd.io/bbolt"
	"gotest.tools/v3/assert"
)

type countTransport struct {
	transport     http.RoundTripper
	recordedTimes []time.Time
}

func (t *countTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.recordedTimes = append(t.recordedTimes, time.Now())
	return t.transport.RoundTrip(req)
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
		t.Parallel()

		dir := t.TempDir()
		// Open dummy DB to prevent server mode.
		db, err := bbolt.Open(filepath.Join(dir, "logs.db"), 0644, nil)
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})
		// Create dummy server address.
		ln, err := net.Listen("tcp", ":0")
		assert.NilError(t, err)
		t.Cleanup(func() {
			_ = ln.Close()
		})
		assert.NilError(t,
			os.WriteFile(filepath.Join(dir, "addr"), []byte("http://"+ln.Addr().String()), 0644),
		)

		var (
			p = backoff.NewConstantPolicy(
				backoff.WithInterval(time.Millisecond*100),
				backoff.WithMaxRetries(5),
			)
			tp = &countTransport{
				transport: http.DefaultTransport,
			}
			c = &http.Client{
				Transport: tp,
				Timeout:   time.Millisecond * 100,
			}
		)

		m, err := New(dir,
			WithRetryPolicy(p),
			WithHTTPClient(c),
		)
		assert.NilError(t, err)
		t.Cleanup(m.Close)

		_, err = m.Resource(background, "fails")
		var ne net.Error
		assert.Assert(t, errors.As(err, &ne) && ne.Timeout())
		assert.Assert(t, len(tp.recordedTimes) >= 5) // Five (re)tries will be recorded.
	})
}

type raceDetector map[string]bool

func (d raceDetector) set(key string) {
	d[key] = true
}

func (d raceDetector) get(key string) bool {
	return d[key]
}

func TestMap_Resource(t *testing.T) {
	t.Parallel()

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

	// MaxParallelism
	// InitFunc
}

func TestRecordStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := bbolt.Open(filepath.Join(dir, "records.db"), 0644, nil)
	assert.NilError(t, err)

	store, err := newRecordStore[initRecord](db)
	assert.NilError(t, err)

	// Record is not stored yet.
	got, err := store.get("treasure")
	assert.ErrorIs(t, err, errRecordNotFound)
	assert.Assert(t, got == nil)

	// Store record.
	assert.NilError(t,
		store.set("treasure", &initRecord{
			Logs: []initLog{
				{
					Event:     initEventStarted,
					Operator:  "alice",
					Timestamp: math.MaxInt64,
				},
			},
		}),
	)

	// Get stored record.
	got, err = store.get("treasure")
	assert.NilError(t, err)
	assert.DeepEqual(t, *got, initRecord{
		Logs: []initLog{
			{
				Event:     initEventStarted,
				Operator:  "alice",
				Timestamp: math.MaxInt64, // Check serialization for large number.
			},
		},
	})

	// Store another record.
	assert.NilError(t,
		store.set("precious", &initRecord{
			Logs: []initLog{
				{
					Event:     initEventStarted,
					Operator:  "bob",
					Timestamp: 1694060338,
				}, {
					Event:     initEventCompleted,
					Operator:  "bob",
					Timestamp: 1694060381,
				},
			},
		}),
	)

	// Iterate records using forEach.
	checkTreasure := mustBeCalledOnce(t, func(t *testing.T, got *initRecord) {
		assert.DeepEqual(t, *got, initRecord{
			Logs: []initLog{
				{
					Event:     initEventStarted,
					Operator:  "alice",
					Timestamp: math.MaxInt64,
				},
			},
		})
	})
	checkPrecious := mustBeCalledOnce(t, func(t *testing.T, got *initRecord) {
		assert.DeepEqual(t, *got, initRecord{
			Logs: []initLog{
				{
					Event:     initEventStarted,
					Operator:  "bob",
					Timestamp: 1694060338,
				}, {
					Event:     initEventCompleted,
					Operator:  "bob",
					Timestamp: 1694060381,
				},
			},
		})
	})
	assert.NilError(t,
		store.forEach(func(name string, obj *initRecord) error {
			switch name {
			case "treasure":
				checkTreasure(t, obj)
			case "precious":
				checkPrecious(t, obj)
			default:
				t.Fatalf("unexpected record found: %#v", obj)
			}
			return nil
		}),
	)
}
