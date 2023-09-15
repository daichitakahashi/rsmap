package logs

import (
	"math"
	"path/filepath"
	"testing"

	"go.etcd.io/bbolt"
	"gotest.tools/v3/assert"
)

func mustBeCalledOnce[T any](t *testing.T, fn func(t *testing.T, v T)) func(t *testing.T, v T) {
	t.Helper()

	var called int
	t.Cleanup(func() {
		t.Helper()

		if called != 1 {
			t.Fatalf("function passed to mustBeCalled has not be called once: %d", called)
		}
	})

	return func(t *testing.T, v T) {
		t.Helper()

		called++
		fn(t, v)
	}
}

func TestRecordStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := bbolt.Open(filepath.Join(dir, "records.db"), 0644, nil)
	assert.NilError(t, err)

	store, err := NewRecordStore[InitRecord](db)
	assert.NilError(t, err)

	// Record is not stored yet.
	got, err := store.Get("treasure")
	assert.ErrorIs(t, err, ErrRecordNotFound)
	assert.Assert(t, got == nil)

	// Store record.
	assert.NilError(t,
		store.Set("treasure", &InitRecord{
			Logs: []InitLog{
				{
					Event:     InitEventStarted,
					Operator:  "alice",
					Timestamp: math.MaxInt64,
				},
			},
		}),
	)

	// Get stored record.
	got, err = store.Get("treasure")
	assert.NilError(t, err)
	assert.DeepEqual(t, *got, InitRecord{
		Logs: []InitLog{
			{
				Event:     InitEventStarted,
				Operator:  "alice",
				Timestamp: math.MaxInt64, // Check serialization for large number.
			},
		},
	})

	// Store another record.
	assert.NilError(t,
		store.Set("precious", &InitRecord{
			Logs: []InitLog{
				{
					Event:     InitEventStarted,
					Operator:  "bob",
					Timestamp: 1694060338,
				}, {
					Event:     InitEventCompleted,
					Operator:  "bob",
					Timestamp: 1694060381,
				},
			},
		}),
	)

	// Iterate records using forEach.
	checkTreasure := mustBeCalledOnce(t, func(t *testing.T, got *InitRecord) {
		assert.DeepEqual(t, *got, InitRecord{
			Logs: []InitLog{
				{
					Event:     InitEventStarted,
					Operator:  "alice",
					Timestamp: math.MaxInt64,
				},
			},
		})
	})
	checkPrecious := mustBeCalledOnce(t, func(t *testing.T, got *InitRecord) {
		assert.DeepEqual(t, *got, InitRecord{
			Logs: []InitLog{
				{
					Event:     InitEventStarted,
					Operator:  "bob",
					Timestamp: 1694060338,
				}, {
					Event:     InitEventCompleted,
					Operator:  "bob",
					Timestamp: 1694060381,
				},
			},
		})
	})
	assert.NilError(t,
		store.ForEach(func(name string, obj *InitRecord) error {
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
