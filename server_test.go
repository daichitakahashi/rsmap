package rsmap

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
	"gotest.tools/v3/assert"
)

func newServerCore(t *testing.T) *serverCore {
	t.Helper()
	dir := t.TempDir()
	db, err := bbolt.Open(filepath.Join(dir, "database.db"), 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	return &serverCore{
		db:        db,
		resources: map[string]*resource{},
	}
}

func TestServerCore_Init(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := newServerCore(t)

	t.Run("complete", func(t *testing.T) {
		t.Parallel()
		name := uuid.NewString()
		started := make(chan struct{})

		for i := 0; i < 30; i++ {
			start := i == 0
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				t.Parallel()
				if !start {
					<-started
				}

				status, err := c.startInit(ctx, name, 100)
				assert.NilError(t, err)
				if start {
					close(started)
					assert.Equal(t, status, statusStarted)
					time.Sleep(time.Second)
					err = c.completeInit(ctx, name, map[string]string{})
					assert.NilError(t, err)
				} else {
					assert.Equal(t, status, statusCompleted)
				}
			})
		}
	})

	t.Run("fail", func(t *testing.T) {
		t.Parallel()
		name := uuid.NewString()
		started := make(chan struct{})

		for i := 0; i < 30; i++ {
			start := i == 0
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				t.Parallel()
				if !start {
					<-started
				}

				status, err := c.startInit(ctx, name, 100)
				assert.NilError(t, err)
				if start {
					close(started)
					assert.Equal(t, status, statusStarted)
					time.Sleep(time.Second)
					err = c.failInit(ctx, name)
					assert.NilError(t, err)
				} else {
					assert.Equal(t, status, statusFailed)
				}
			})
		}
	})
}
