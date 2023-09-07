package rsmap

import (
	"context"
	"testing"
)

//go:generate go run go.uber.org/mock/mockgen@latest -source ./log.go -destination ./mock_test.go -package rsmap -typed keyValueStore

var background = context.Background()

func asyncResult[T any](fn func() T) (result <-chan T) {
	ch := make(chan T)
	go func() {
		ch <- fn()
	}()
	return ch
}

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
