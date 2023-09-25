package ctl_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"gotest.tools/v3/assert"

	"github.com/daichitakahashi/rsmap/internal/ctl"
)

var background = context.Background()

type safeBuffer struct {
	*bytes.Buffer
	m sync.Mutex
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.m.Lock()
	defer s.m.Unlock()
	return s.Buffer.Write(p)
}

func TestInitCtl(t *testing.T) {
	t.Parallel()

	t.Run("init must be performed once", func(t *testing.T) {
		t.Parallel()

		var (
			ctl   = ctl.NewInitCtl(false)
			wg    sync.WaitGroup
			begin = make(chan struct{})
			eg    errgroup.Group
			out   = &safeBuffer{
				Buffer: bytes.NewBuffer(nil),
			}
		)

		wg.Add(10)
		for i := 0; i < 10; i++ {
			operator := strconv.Itoa(i)
			eg.Go(func() error {
				wg.Done()
				<-begin

				result := <-ctl.TryInit(background, operator)
				if result.Err != nil {
					return result.Err
				}
				if result.Try {
					time.Sleep(time.Second)
					fmt.Fprintln(out, "Start initialization!")

					err := ctl.Complete(operator)
					if err != nil {
						return err
					}
					fmt.Fprintln(out, "Completed.")
				} else {
					fmt.Fprintln(out, "Already initialized.")
				}
				return nil
			})
		}

		// Rendezvous.
		wg.Wait()
		close(begin)

		// Check result.
		assert.NilError(t, eg.Wait())
		assert.DeepEqual(t, out.String(), `Start initialization!
Completed.
Already initialized.
Already initialized.
Already initialized.
Already initialized.
Already initialized.
Already initialized.
Already initialized.
Already initialized.
Already initialized.
`)
	})

	t.Run("retry init after failure", func(t *testing.T) {
		t.Parallel()

		var (
			ctl   = ctl.NewInitCtl(false)
			wg    sync.WaitGroup
			eg    errgroup.Group
			begin = make(chan struct{})
		)

		wg.Add(2)

		// First try succeeds.
		eg.Go(func() error {
			wg.Done()
			<-begin

			result := <-ctl.TryInit(background, "alice")
			if result.Err != nil {
				return result.Err
			}
			if !result.Try {
				return errors.New("first try must succeeds")
			}

			// Set first try failed.
			return ctl.Fail("alice")
		})

		// Also, second try succeeds after the failure of first try.
		eg.Go(func() error {
			wg.Done()
			<-begin
			time.Sleep(time.Millisecond * 200)

			result := <-ctl.TryInit(background, "bob")
			if result.Err != nil {
				return result.Err
			}
			if !result.Try {
				return errors.New("second try must succeeds")
			}
			return nil
		})

		// Rendezvous.
		wg.Wait()
		close(begin)

		assert.NilError(t, eg.Wait())
		assert.NilError(t, ctl.Complete("bob"))
	})

	t.Run("consecutive try by same operator", func(t *testing.T) {
		t.Parallel()

		ctl := ctl.NewInitCtl(false)
		result := <-ctl.TryInit(background, "alice")
		assert.NilError(t, result.Err)
		assert.Assert(t, result.Initiated && result.Try)

		result = <-ctl.TryInit(background, "alice")
		assert.Assert(t, !result.Initiated && result.Try)

		assert.NilError(t, ctl.Complete("alice"))
	})

	t.Run("already completed", func(t *testing.T) {
		t.Parallel()

		ctl := ctl.NewInitCtl(false)
		result := <-ctl.TryInit(background, "alice")
		assert.NilError(t, result.Err)
		assert.Assert(t, result.Initiated && result.Try)

		assert.NilError(t, ctl.Complete("alice"))

		// Already completed.
		result = <-ctl.TryInit(background, "alice")
		assert.NilError(t, result.Err)
		assert.Assert(t, !result.Initiated && !result.Try)

		result = <-ctl.TryInit(background, "bob")
		assert.NilError(t, result.Err)
		assert.Assert(t, !result.Initiated && !result.Try)
	})

	t.Run("replay completed", func(t *testing.T) {
		t.Parallel()

		ctl := ctl.NewInitCtl(true)
		result := <-ctl.TryInit(background, "alice")
		assert.NilError(t, result.Err)
		assert.Assert(t, !result.Initiated && !result.Try)
	})

	t.Run("invalid operator", func(t *testing.T) {
		t.Parallel()

		ctl := ctl.NewInitCtl(false)
		result := <-ctl.TryInit(background, "alice")
		assert.NilError(t, result.Err)
		assert.Assert(t, result.Initiated && result.Try)

		// Complete by Bob.
		err := ctl.Complete("bob")
		assert.Assert(t, err != nil)

		// Fail by Bob.
		err = ctl.Fail("bob")
		assert.Assert(t, err != nil)

		// Complete by Alice.
		assert.NilError(t, ctl.Complete("alice"))
	})

	t.Run("context canceled", func(t *testing.T) {
		t.Parallel()

		ctl := ctl.NewInitCtl(false)

		result := <-ctl.TryInit(background, "alice")
		assert.NilError(t, result.Err)
		assert.Assert(t, result.Initiated && result.Try)

		ctx, cancel := context.WithTimeout(background, time.Millisecond*100)
		defer cancel()
		result = <-ctl.TryInit(ctx, "bob")
		assert.ErrorIs(t, result.Err, context.DeadlineExceeded)
		assert.Assert(t, !result.Initiated && !result.Try)

		assert.NilError(t, ctl.Complete("alice"))
	})
}
