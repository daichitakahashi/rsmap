package rsmap

import (
	"math"
	"path/filepath"
	"testing"

	"go.etcd.io/bbolt"
	"gotest.tools/v3/assert"
)

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
