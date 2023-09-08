package rsmap

// import (
// 	"context"
// 	"math/rand"
// 	"sync"
// 	"testing"
// 	"time"

// 	"golang.org/x/sync/errgroup"
// )

// func TestConnect(t *testing.T) {
// 	t.Parallel()

// 	var m sync.Mutex
// 	sleep := func() {
// 		m.Lock()
// 		d := time.Duration(rand.Intn(100)) * time.Millisecond
// 		m.Unlock()
// 		time.Sleep(d)
// 	}

// 	eg, ctx := errgroup.WithContext(context.Background())
// 	for i := 0; i < 1000; i++ {
// 		eg.Go(func() error {
// 			sleep()

// 			closeFn, err := connect(ctx, 100)
// 			if err != nil {
// 				return err
// 			}
// 			sleep()
// 			defer closeFn()
// 			return nil
// 		})
// 	}
// 	err := eg.Wait()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// }
