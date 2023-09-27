package ctl

import (
	"context"
	"errors"
	"sync"
)

type (
	// InitCtl is a primitive for controlling init status.
	InitCtl struct {
		_lock      chan struct{}
		_m         sync.RWMutex
		_completed bool
		_operator  string
	}

	TryInitResult struct {
		Try       bool
		Initiated bool
		Err       error
	}
)

// NewInitCtl creates new InitCtl.
func NewInitCtl(completed bool) *InitCtl {
	return &InitCtl{
		_lock:      make(chan struct{}, 1), // Allocate minimum buffer.
		_completed: completed,
	}
}

// TryInit tries to acquire lock and start init operation.
// When operation is already completed, 'try' will be false.
func (i *InitCtl) TryInit(ctx context.Context, operator string) <-chan TryInitResult {
	try := make(chan TryInitResult, 1)
	i._m.RLock()
	completed, op := i._completed, i._operator
	i._m.RUnlock()

	if completed {
		try <- TryInitResult{}
		return try
	}

	// If the operator acquires lock for init but the fact is not recognized by operator,
	// give a second chance to try.
	if !completed && op == operator {
		try <- TryInitResult{
			Try:       true,
			Initiated: false,
		}
		return try
	}

	go func() {
		select {
		case <-ctx.Done():
			try <- TryInitResult{
				Try: false,
				Err: ctx.Err(),
			}
			return
		case i._lock <- struct{}{}:
			i._m.Lock()
			defer i._m.Unlock()

			if i._completed {
				try <- TryInitResult{
					Try: false,
				}
				<-i._lock // Release.
				return
			}

			// Set current operator.
			i._operator = operator
			try <- TryInitResult{
				Try:       true,
				Initiated: true,
				Err:       nil,
			}

			// Keep locked.
		}
	}()
	return try
}

// Complete marks init operation as completed.
func (i *InitCtl) Complete(operator string) error {
	i._m.Lock()
	defer i._m.Unlock()

	if i._operator != operator {
		return errors.New("invalid operation")
	}

	i._completed = true
	<-i._lock // Release.
	return nil
}

// Fail marks init operation as failed.
func (i *InitCtl) Fail(operator string) error {
	i._m.Lock()
	defer i._m.Unlock()

	if i._operator != operator {
		return errors.New("invalid operation")
	}

	<-i._lock // Release.
	return nil
}
