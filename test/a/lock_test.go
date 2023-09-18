package a

import (
	"fmt"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/daichitakahashi/rsmap"
	"github.com/daichitakahashi/rsmap/test"
)

func TestLock(t *testing.T) {
	t.Parallel()

	for i := 0; i < 2; i++ {
		op := "lock"
		if i%2 == 1 {
			op = "rlock"
		}
		t.Run(fmt.Sprintf("%s_%d", op, i), func(t *testing.T) {
			t.Parallel()

			m, err := rsmap.New(".rsmap")
			assert.NilError(t, err)
			t.Cleanup(m.Close)

			r, err := m.Resource(test.Context(t), test.Resource, test.Options()...)
			assert.NilError(t, err)

			if op == "lock" {
				assert.NilError(t, r.Lock(test.Context(t)))
			} else {
				assert.NilError(t, r.RLock(test.Context(t)))
			}
			t.Cleanup(func() {
				assert.NilError(t, r.UnlockAny())
			})

			test.DoSomething()
		})
	}
}
