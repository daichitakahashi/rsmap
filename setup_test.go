package rsmap

import "context"

//go:generate go run go.uber.org/mock/mockgen@latest -source ./log.go -destination ./mock_test.go -package rsmap -typed keyValueStore

var background = context.Background()

func asyncResult[T any](fn func() T) (result <-chan T) {
	ch := make(chan T)
	go func() {
		ch <- fn()
	}()
	return ch
}
