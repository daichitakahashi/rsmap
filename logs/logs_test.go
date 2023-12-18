package logs

import (
	"math"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"
	"go.etcd.io/bbolt"
	"gotest.tools/v3/assert"

	logsv1 "github.com/daichitakahashi/rsmap/internal/proto/logs/v1"
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

var ignoreProtoUnexported = cmpopts.IgnoreUnexported(
	logsv1.Caller{},
	logsv1.ServerRecord{},
	logsv1.ServerLog{},
	logsv1.InitRecord{},
	logsv1.InitLog{},
	logsv1.AcquisitionRecord{},
	logsv1.AcquisitionLog{},
)

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
		assert.NilError(t, store.PutServerLog(&logsv1.ServerLog{
			Event: logsv1.ServerEvent_SERVER_EVENT_LAUNCHED,
			Addr:  "http://localhost:8080",
			Context: []*logsv1.Caller{
				{
					File: "alice.go",
					Line: 10,
					Hash: "4e5704d7",
				},
			},
			Timestamp: 1694765593803865000,
		}))
		assert.NilError(t, store.PutServerLog(&logsv1.ServerLog{
			Event: logsv1.ServerEvent_SERVER_EVENT_STOPPED,
			Context: []*logsv1.Caller{
				{
					File: "bob.go",
					Line: 124,
					Hash: "9e9e9f3c",
				},
			},
			Timestamp: 1694765603344265000,
		}))

		// Get server logs(actually, not stored).
		assert.DeepEqual(t, store.ServerRecord(), &logsv1.ServerRecord{
			Logs: []*logsv1.ServerLog{
				{
					Event: logsv1.ServerEvent_SERVER_EVENT_LAUNCHED,
					Addr:  "http://localhost:8080",
					Context: []*logsv1.Caller{
						{
							File: "alice.go",
							Line: 10,
							Hash: "4e5704d7",
						},
					},
					Timestamp: 1694765593803865000,
				},
				{
					Event: logsv1.ServerEvent_SERVER_EVENT_STOPPED,
					Context: []*logsv1.Caller{
						{
							File: "bob.go",
							Line: 124,
							Hash: "9e9e9f3c",
						},
					},
					Timestamp: 1694765603344265000,
				},
			},
		}, ignoreProtoUnexported)
	}()

	db, err := bbolt.Open(filepath.Join(dir, "records.db"), 0644, nil)
	assert.NilError(t, err)
	defer func() {
		_ = db.Close()
	}()

	store, err := NewInfoStore(db)
	assert.NilError(t, err)

	// Get stored server logs.
	assert.DeepEqual(t, store.ServerRecord(), &logsv1.ServerRecord{
		Logs: []*logsv1.ServerLog{
			{
				Event: logsv1.ServerEvent_SERVER_EVENT_LAUNCHED,
				Addr:  "http://localhost:8080",
				Context: []*logsv1.Caller{
					{
						File: "alice.go",
						Line: 10,
						Hash: "4e5704d7",
					},
				},
				Timestamp: 1694765593803865000,
			},
			{
				Event: logsv1.ServerEvent_SERVER_EVENT_STOPPED,
				Context: []*logsv1.Caller{
					{
						File: "bob.go",
						Line: 124,
						Hash: "9e9e9f3c",
					},
				},
				Timestamp: 1694765603344265000,
			},
		},
	}, ignoreProtoUnexported)
}

func TestResourceRecordStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := bbolt.Open(filepath.Join(dir, "records.db"), 0644, nil)
	assert.NilError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	store, err := NewResourceRecordStore[logsv1.InitRecord](db)
	assert.NilError(t, err)

	// Record is not stored yet.
	got, err := store.Get("treasure")
	assert.ErrorIs(t, err, ErrRecordNotFound)
	assert.Assert(t, got == nil)

	// Store record.
	assert.NilError(t,
		store.Put([]string{"treasure"}, func(_ string, r *logsv1.InitRecord, update bool) {
			// This is a new record.
			assert.Assert(t, !update)

			r.Logs = append(r.Logs, &logsv1.InitLog{
				Event: logsv1.InitEvent_INIT_EVENT_STARTED,
				Context: []*logsv1.Caller{
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
	assert.DeepEqual(t, got, &logsv1.InitRecord{
		Logs: []*logsv1.InitLog{
			{
				Event: logsv1.InitEvent_INIT_EVENT_STARTED,
				Context: []*logsv1.Caller{
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
	}, ignoreProtoUnexported)

	// Store another record.
	assert.NilError(t, store.Put([]string{"precious"}, func(_ string, r *logsv1.InitRecord, update bool) {
		assert.Assert(t, !update)

		r.Logs = append(r.Logs, &logsv1.InitLog{
			Event: logsv1.InitEvent_INIT_EVENT_STARTED,
			Context: []*logsv1.Caller{
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
	assert.NilError(t, store.Put([]string{"precious"}, func(_ string, r *logsv1.InitRecord, update bool) {
		// Update record.
		assert.Assert(t, update)

		r.Logs = append(r.Logs, &logsv1.InitLog{
			Event: logsv1.InitEvent_INIT_EVENT_COMPLETED,
			Context: []*logsv1.Caller{
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
	checkTreasure := mustBeCalledOnce(t, func(t *testing.T, got *logsv1.InitRecord) {
		assert.DeepEqual(t, got, &logsv1.InitRecord{
			Logs: []*logsv1.InitLog{
				{
					Event: logsv1.InitEvent_INIT_EVENT_STARTED,
					Context: []*logsv1.Caller{
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
		}, ignoreProtoUnexported)
	})
	checkPrecious := mustBeCalledOnce(t, func(t *testing.T, got *logsv1.InitRecord) {
		assert.DeepEqual(t, got, &logsv1.InitRecord{
			Logs: []*logsv1.InitLog{
				{
					Event: logsv1.InitEvent_INIT_EVENT_STARTED,
					Context: []*logsv1.Caller{
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
					Event: logsv1.InitEvent_INIT_EVENT_COMPLETED,
					Context: []*logsv1.Caller{
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
		}, ignoreProtoUnexported)
	})
	assert.NilError(t,
		store.ForEach(func(name string, obj *logsv1.InitRecord) error {
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
