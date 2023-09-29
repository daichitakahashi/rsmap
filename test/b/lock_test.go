package b

import (
	"testing"

	"gotest.tools/v3/assert"

	"github.com/daichitakahashi/rsmap"
	"github.com/daichitakahashi/rsmap/test"
)

func Test_Treasure(t *testing.T) {
	t.Parallel()

	for name, op := range test.Operations(10) {
		op := op
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m, err := rsmap.New(test.Dir())
			assert.NilError(t, err)
			t.Cleanup(m.Close)

			r, err := m.Resource(test.Context(t), test.ResourceTreasure, test.Options()...)
			assert.NilError(t, err)

			if op == test.OpLock {
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

func Test_Precious(t *testing.T) {
	t.Parallel()

	for name, op := range test.Operations(10) {
		op := op
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m, err := rsmap.New(test.Dir())
			assert.NilError(t, err)
			t.Cleanup(m.Close)

			r, err := m.Resource(test.Context(t), test.ResourcePrecious, test.Options()...)
			assert.NilError(t, err)

			if op == test.OpLock {
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
