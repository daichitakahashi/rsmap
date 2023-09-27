package testutil

import (
	"bytes"
	"sync"
)

type SafeBuffer struct {
	*bytes.Buffer
	m sync.Mutex
}

func NewSafeBuffer() *SafeBuffer {
	return &SafeBuffer{
		Buffer: bytes.NewBuffer(nil),
	}
}

func (s *SafeBuffer) Write(p []byte) (int, error) {
	s.m.Lock()
	defer s.m.Unlock()
	return s.Buffer.Write(p)
}
