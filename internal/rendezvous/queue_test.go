package rendezvous_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"gotest.tools/v3/assert"

	"github.com/daichitakahashi/rsmap/internal/rendezvous"
	"github.com/daichitakahashi/rsmap/internal/testutil"
)

func TestLimitedTermQueue(t *testing.T) {
	t.Parallel()

	t.Run("correct order", func(t *testing.T) {
		t.Parallel()

		b := rendezvous.NewBuilder()
		b.Add("alice")
		b.Add("bob")
		b.Add("charlie")
		q := b.Start(time.Second)

		var (
			eg    errgroup.Group
			wg    sync.WaitGroup
			begin = make(chan struct{})
			out   = testutil.NewSafeBuffer()
		)
		wg.Add(3)
		eg.Go(func() error {
			wg.Done()
			<-begin

			time.Sleep(time.Millisecond * 400)
			q.Dequeue("alice", func(dequeue bool) {
				if !dequeue {
					panic("dequeue: alice failed")
				}
				fmt.Fprintln(out, "alice")
			})
			return nil
		})
		eg.Go(func() error {
			wg.Done()
			<-begin

			time.Sleep(time.Millisecond * 200)
			q.Dequeue("bob", func(dequeue bool) {
				if !dequeue {
					panic("dequeue: bob failed")
				}
				fmt.Fprintln(out, "bob")
			})
			return nil
		})
		eg.Go(func() error {
			wg.Done()
			<-begin

			time.Sleep(time.Millisecond * 100)
			q.Dequeue("charlie", func(dequeue bool) {
				if !dequeue {
					panic("dequeue: charlie failed")
				}
				fmt.Fprintln(out, "charlie")
			})
			return nil
		})

		// Rendezvous.
		wg.Wait()
		close(begin)
		assert.NilError(t, eg.Wait())

		/// Check output order.
		assert.DeepEqual(t, out.String(), "alice\nbob\ncharlie\n")
	})

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()

		b := rendezvous.NewBuilder()
		b.Add("alice")
		b.Add("bob")
		q := b.Start(time.Millisecond * 100)

		time.Sleep(time.Millisecond * 400)
		q.Dequeue("charlie", func(dequeue bool) {
			assert.Assert(t, !dequeue)
		})
	})

	t.Run("remove", func(t *testing.T) {
		t.Parallel()

		b := rendezvous.NewBuilder()
		b.Add("alice")
		b.Remove("alice") // Alice is removed from queue.
		b.Add("bob")
		q := b.Start(time.Hour)

		out := testutil.NewSafeBuffer()

		q.Dequeue("bob", func(dequeue bool) {
			assert.Assert(t, dequeue)
			fmt.Fprintln(out, "bob")
		})
		q.Dequeue("charlie", func(dequeue bool) {
			assert.Assert(t, !dequeue)
			fmt.Fprintln(out, "charlie")
		})

		assert.DeepEqual(t, out.String(), "bob\ncharlie\n")
	})
}

func TestEmptyQueue(t *testing.T) {
	t.Parallel()

	b := rendezvous.NewBuilder()
	q := b.Start(time.Second)

	var called bool
	q.Dequeue("alice", func(dequeue bool) {
		assert.Assert(t, !dequeue)
		called = true
	})
	assert.Assert(t, called)
}
