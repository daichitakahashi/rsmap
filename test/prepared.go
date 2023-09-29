package test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/daichitakahashi/rsmap"
)

func Dir() string {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	return filepath.Join(dir, ".rsmap")
}

const (
	ResourceTreasure = "treasure"
	ResourcePrecious = "precious"
)

func Options() []*rsmap.ResourceOption {
	return []*rsmap.ResourceOption{
		rsmap.WithMaxParallelism(3),
		rsmap.WithInit(func(ctx context.Context) error {
			DoSomething()
			return nil
		}),
	}
}

func DoSomething() {
	time.Sleep(
		time.Duration(rand.Intn(2)) * time.Second,
	)
}

func Context(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

type Op int

const (
	OpLock  Op = 1
	OpRLock Op = 2
)

func Operations(n int) map[string]Op {
	m := map[string]Op{}

	for i := 0; i < n; i++ {
		if i%2 == 0 {
			m[fmt.Sprintf("Lock_%d", i/2)] = OpLock
		} else {
			m[fmt.Sprintf("RLock_%d", i/2)] = OpRLock
		}
	}

	return m
}
