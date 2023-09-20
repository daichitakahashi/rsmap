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

func TestInfoStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	func() {
		db, err := bbolt.Open(filepath.Join(dir, "records.db"), 0644, nil)
		assert.NilError(t, err)
		defer func() {
			_ = db.Close()
		}()

		store, err := NewInfoStore(db)
		assert.NilError(t, err)

		// Store server logs.
		assert.NilError(t, store.PutServerLog(ServerLog{
			Event: ServerEventLaunched,
			Addr:  "http://localhost:8080",
			Context: CallerContext{
				{
					File: "alice.go",
					Line: 10,
					Hash: "4e5704d7",
				},
			},
			Timestamp: 1694765593803865000,
		}))
		assert.NilError(t, store.PutServerLog(ServerLog{
			Event: ServerEventStopped,
			Context: CallerContext{
				{
					File: "bob.go",
					Line: 124,
					Hash: "9e9e9f3c",
				},
			},
			Timestamp: 1694765603344265000,
		}))

		// Get server logs(actually, not stored).
		assert.DeepEqual(t, *store.ServerRecord(), ServerRecord{
			Logs: []ServerLog{
				{
					Event: ServerEventLaunched,
					Addr:  "http://localhost:8080",
					Context: CallerContext{
						{
							File: "alice.go",
							Line: 10,
							Hash: "4e5704d7",
						},
					},
					Timestamp: 1694765593803865000,
				},
				{
					Event: ServerEventStopped,
					Context: CallerContext{
						{
							File: "bob.go",
							Line: 124,
							Hash: "9e9e9f3c",
						},
					},
					Timestamp: 1694765603344265000,
				},
			},
		})
	}()

	db, err := bbolt.Open(filepath.Join(dir, "records.db"), 0644, nil)
	assert.NilError(t, err)
	defer func() {
		_ = db.Close()
	}()

	store, err := NewInfoStore(db)
	assert.NilError(t, err)

	// Get stored server logs.
	assert.DeepEqual(t, *store.ServerRecord(), ServerRecord{
		Logs: []ServerLog{
			{
				Event: ServerEventLaunched,
				Addr:  "http://localhost:8080",
				Context: CallerContext{
					{
						File: "alice.go",
						Line: 10,
						Hash: "4e5704d7",
					},
				},
				Timestamp: 1694765593803865000,
			},
			{
				Event: ServerEventStopped,
				Context: CallerContext{
					{
						File: "bob.go",
						Line: 124,
						Hash: "9e9e9f3c",
					},
				},
				Timestamp: 1694765603344265000,
			},
		},
	})
}

func TestResourceRecordStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := bbolt.Open(filepath.Join(dir, "records.db"), 0644, nil)
	assert.NilError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	store, err := NewResourceRecordStore[InitRecord](db)
	assert.NilError(t, err)

	// Record is not stored yet.
	got, err := store.Get("treasure")
	assert.ErrorIs(t, err, ErrRecordNotFound)
	assert.Assert(t, got == nil)

	// Store record.
	assert.NilError(t,
		store.Put("treasure", func(r *InitRecord, update bool) {
			// This is a new record.
			assert.Assert(t, !update)

			r.Logs = append(r.Logs, InitLog{
				Event: InitEventStarted,
				Context: CallerContext{
					{
						File: "alice.go",
						Line: 77,
						Hash: "b6830588",
					},
					{
						File: "alice.go",
						Line: 342,
						Hash: "e45ecc5c",
					},
				},
				Timestamp: math.MaxInt64,
			})
		}),
	)

	// Get stored record.
	got, err = store.Get("treasure")
	assert.NilError(t, err)
	assert.DeepEqual(t, *got, InitRecord{
		Logs: []InitLog{
			{
				Event: InitEventStarted,
				Context: CallerContext{
					{
						File: "alice.go",
						Line: 77,
						Hash: "b6830588",
					},
					{
						File: "alice.go",
						Line: 342,
						Hash: "e45ecc5c",
					},
				},
				Timestamp: math.MaxInt64, // Check serialization for large number.
			},
		},
	})

	// Store another record.
	assert.NilError(t, store.Put("precious", func(r *InitRecord, update bool) {
		assert.Assert(t, !update)

		r.Logs = append(r.Logs, InitLog{
			Event: InitEventStarted,
			Context: CallerContext{
				{
					File: "bob.go",
					Line: 12,
					Hash: "335f4b5d",
				},
				{
					File: "bob.go",
					Line: 76,
					Hash: "1b42d1e9",
				},
			},
			Timestamp: 1694765621790751000,
		})
	}))
	assert.NilError(t, store.Put("precious", func(r *InitRecord, update bool) {
		// Update record.
		assert.Assert(t, update)

		r.Logs = append(r.Logs, InitLog{
			Event: InitEventCompleted,
			Context: CallerContext{
				{
					File: "bob.go",
					Line: 12,
					Hash: "335f4b5d",
				},
				{
					File: "bob.go",
					Line: 149,
					Hash: "2c9c21db",
				},
			},
			Timestamp: 1694765637968901000,
		})
	}))

	// Iterate records using forEach.
	checkTreasure := mustBeCalledOnce(t, func(t *testing.T, got *InitRecord) {
		assert.DeepEqual(t, *got, InitRecord{
			Logs: []InitLog{
				{
					Event: InitEventStarted,
					Context: CallerContext{
						{
							File: "alice.go",
							Line: 77,
							Hash: "b6830588",
						},
						{
							File: "alice.go",
							Line: 342,
							Hash: "e45ecc5c",
						},
					},
					Timestamp: math.MaxInt64,
				},
			},
		})
	})
	checkPrecious := mustBeCalledOnce(t, func(t *testing.T, got *InitRecord) {
		assert.DeepEqual(t, *got, InitRecord{
			Logs: []InitLog{
				{
					Event: InitEventStarted,
					Context: CallerContext{
						{
							File: "bob.go",
							Line: 12,
							Hash: "335f4b5d",
						},
						{
							File: "bob.go",
							Line: 76,
							Hash: "1b42d1e9",
						},
					},
					Timestamp: 1694765621790751000,
				}, {
					Event: InitEventCompleted,
					Context: CallerContext{
						{
							File: "bob.go",
							Line: 12,
							Hash: "335f4b5d",
						},
						{
							File: "bob.go",
							Line: 149,
							Hash: "2c9c21db",
						},
					},
					Timestamp: 1694765637968901000,
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
