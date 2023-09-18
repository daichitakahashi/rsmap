package test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/daichitakahashi/rsmap"
)

const Resource = "resource"

func Options() []*rsmap.ResourceOption {
	return []*rsmap.ResourceOption{
		rsmap.WithMaxParallelism(3),
		rsmap.WithInit(func(ctx context.Context) {
			DoSomething()
		}),
	}
}

func DoSomething() {
	time.Sleep(
		time.Duration(rand.Intn(5)) * time.Second,
	)
}

func Context(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	t.Cleanup(cancel)
	return ctx
}
