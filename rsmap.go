package rsmap

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type (
	ResourceMap struct {
		core
		resources sync.Map
	}

	ReleaseFunc func() error

	core interface {
		startInit(ctx context.Context, resourceName string, max int64) (initStatus, error)
		completeInit(ctx context.Context, resourceName string, data any) error
		failInit(ctx context.Context, resourceName string) error
		useExclusive(ctx context.Context, resourceName, acquisitionID string, v any) error
		useShared(ctx context.Context, resourceName, acquisitionID string, v any) error
		release(ctx context.Context, resourceName, acquisitionID string) error
	}
)

func (r *ResourceMap) SetResource(ctx context.Context, name string, init func(context.Context) (any, error)) error {
	const max = 999999999 // TODO: make it option

	status, err := r.startInit(ctx, name, max)
	if err != nil {
		return err
	}

	switch status {
	case statusStarted:
		data, err := init(ctx)
		if err != nil {
			err = errors.Join(
				r.failInit(ctx, name),
			)
			return fmt.Errorf("error on init func: %w", err)
		}
		err = r.completeInit(ctx, name, data)
		if err != nil {
			return err
		}
	case statusFailed:
		return errors.New("resource is not initialized correctly")
	case statusCompleted:
	}

	r.resources.Store(name, true)

	return nil
}

func (r *ResourceMap) UseExclusive(ctx context.Context, resourceName string, v any) (ReleaseFunc, error) {
	if _, ok := r.resources.Load(resourceName); !ok {
		return nil, errors.New("resource is not set")
	}

	id := uuid.NewString()
	err := r.useExclusive(ctx, resourceName, id, v)
	if err != nil {
		return nil, err
	}
	return func() error {
		return r.release(context.Background(), resourceName, id)
	}, nil
}

func (r *ResourceMap) UseShared(ctx context.Context, resourceName string, v any) (ReleaseFunc, error) {
	if _, ok := r.resources.Load(resourceName); !ok {
		return nil, errors.New("resource is not set")
	}

	id := uuid.NewString()
	err := r.useShared(ctx, resourceName, id, v)
	if err != nil {
		return nil, err
	}
	return func() error {
		return r.release(context.Background(), resourceName, id)
	}, nil
}
