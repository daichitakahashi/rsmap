package rsmap

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"gotest.tools/v3/assert"
)

var background = context.Background()

func TestInitCtl(t *testing.T) {
	t.Parallel()

	var (
		ctl    = newInitCtl(false)
		begin  = make(chan struct{})
		eg     errgroup.Group
		result = bytes.NewBuffer(nil)
	)

	for i := 0; i < 10; i++ {
		operator := strconv.Itoa(i)
		eg.Go(func() error {
			<-begin

			var started bool
			err := ctl.tryInit(background, operator, func(try bool) error {
				if try {
					time.Sleep(time.Second)
					fmt.Fprintln(result, "Start initialization!")
					started = true
					return nil
				} else {
					fmt.Fprintln(result, "Already initialized.")
				}
				return nil
			})
			if err != nil {
				return err
			}

			if started {
				return ctl.complete(operator, func() error {
					fmt.Fprintln(result, "Completed.")
					return nil
				})
			}
			return nil
		})
	}

	close(begin)
	assert.NilError(t, eg.Wait())
	assert.DeepEqual(t, result.String(), `Start initialization!
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
}
