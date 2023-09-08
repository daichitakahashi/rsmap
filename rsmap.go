package rsmap

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lestrrat-go/backoff/v2"
	"github.com/lestrrat-go/option"
	"go.etcd.io/bbolt"
)

type (
	// Map is the controller for external resource usage.
	Map struct {
		_clientID string
		_cfg      config
		_mu       sync.RWMutex
		_rm       resourceMap
		_stop     func()
	}

	// Resource brings an ability of acquire/release lock for the dedicated resource.
	Resource struct {
		_m *Map
		_n int64
	}
)

func New() *Map {
	// TODO: make optional
	cfg := config{
		dbFile:   "db.db",
		addrFile: "addr",
		retryPolicy: backoff.NewConstantPolicy(
			backoff.WithMaxRetries(10),
			backoff.WithInterval(time.Millisecond*100),
		),
		httpCli: http.DefaultClient,
	}

	m := &Map{
		_clientID: uuid.NewString(),
		_cfg:      cfg,
		_rm:       newClientSideMap(cfg),
	}

	// Start server launch process, and set release function.
	m._stop = m.launchServer()

	return m
}

func (m *Map) Close() {
	m._stop()
}

type (
	// ResourceOption represents option for [(*Map).Resource]
	ResourceOption struct {
		option.Interface
	}
	identOptionParallelism struct{}
	identOptionInit        struct{}
)

// WithParallelism can specify max parallelism.
// Default value is 5.
func WithParallelism(n int64) *ResourceOption {
	return &ResourceOption{
		Interface: option.New(identOptionParallelism{}, n),
	}
}

// TODO: add error as a return value.
type InitFunc func(ctx context.Context)

// WithInit specifies InitFunc for resource initialization.
//
// InitFunc will be called only once globally, at first declaration by [(*Map).Resource].
// Other process waits until the completion of this initialization.
// So, if Resource is called without this option, we cannot perform initializations with concurrency safety.
func WithInit(init InitFunc) *ResourceOption {
	return &ResourceOption{
		Interface: option.New(identOptionInit{}, init),
	}
}

// Resource creates [Resource] object that provides control for resource usage.
func (m *Map) Resource(ctx context.Context, name string, opts ...*ResourceOption) (*Resource, error) {
	var (
		n    = int64(5)
		init InitFunc
	)

	// Check options.
	for _, opt := range opts {
		switch opt.Ident() {
		case identOptionParallelism{}:
			n = opt.Value().(int64)
		case identOptionInit{}:
			init = opt.Value().(InitFunc)
		}
	}
	m._mu.RLock()
	try, err := m._rm.tryInit(ctx, name, m._clientID)
	m._mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if try {
		if init != nil {
			// Initialization of the resource.
			init(ctx)

			// TODO: error handling.

			m._mu.RLock()
			err = m._rm.completeInit(ctx, name, m._clientID)
			m._mu.RUnlock()
			if err != nil {
				return nil, err
			}
		}
	}

	return &Resource{
		_m: m,
		_n: n,
	}, nil
}

// Core interface for control operations for both server and client side.
type resourceMap interface {
	tryInit(ctx context.Context, resourceName, operator string) (bool, error)
	completeInit(ctx context.Context, resourceName, operator string) error
	acquire(ctx context.Context, resourceName, operator string, max int64, exclusive bool) error
	release(ctx context.Context, resourceName, operator string) error
}

type serverSideMap struct {
	_init    *initController
	_acquire *acquireController
}

// Create resourceMap for server side.
// This map reads and updates bbolt.DB directly.
func newServerSideMap(db *bbolt.DB) (*serverSideMap, error) {
	initRecordStore, err := newRecordStore[initRecord](db)
	if err != nil {
		return nil, err
	}
	acquireRecordStore, err := newRecordStore[acquireRecord](db)
	if err != nil {
		return nil, err
	}

	init, err := loadInitController(initRecordStore)
	if err != nil {
		return nil, err
	}
	acquire, err := loadAcquireController(acquireRecordStore)
	if err != nil {
		return nil, err
	}

	return &serverSideMap{
		_init:    init,
		_acquire: acquire,
	}, nil
}

func (m *serverSideMap) tryInit(ctx context.Context, resourceName, operator string) (bool, error) {
	return m._init.tryInit(ctx, resourceName, operator)
}

func (m *serverSideMap) completeInit(_ context.Context, resourceName, operator string) error {
	return m._init.complete(resourceName, operator)
}

func (m *serverSideMap) acquire(ctx context.Context, resourceName string, operator string, max int64, exclusive bool) error {
	return m._acquire.acquire(ctx, resourceName, operator, max, exclusive)
}

func (m *serverSideMap) release(_ context.Context, resourceName, operator string) error {
	return m._acquire.release(resourceName, operator)
}

var _ resourceMap = (*serverSideMap)(nil)

// keyValueStore implementation for serverSideMap.
type recordStore[T initRecord | acquireRecord] struct {
	_bucketName []byte
	_db         *bbolt.DB
}

func newRecordStore[T initRecord | acquireRecord](db *bbolt.DB) (*recordStore[T], error) {
	var (
		t          T
		v          any = t
		bucketName []byte
	)
	switch v.(type) {
	case initRecord:
		bucketName = []byte("init")
	case acquireRecord:
		bucketName = []byte("acquire")
	}

	err := db.Update(func(tx *bbolt.Tx) error {
		// Create bucket for records.
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &recordStore[T]{
		_bucketName: bucketName,
		_db:         db,
	}, err
}

func (s *recordStore[T]) forEach(fn func(name string, obj *T) error) error {
	return s._db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(s._bucketName).ForEach(func(k, v []byte) error {
			var r T
			err := json.Unmarshal(v, &r)
			if err != nil {
				return err
			}
			return fn(string(k), &r)
		})
	})
}

func (s *recordStore[T]) get(name string) (*T, error) {
	var r T
	err := s._db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(s._bucketName).Get([]byte(name))
		if data == nil {
			return errRecordNotFound
		}
		return json.Unmarshal(data, &r)
	})
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *recordStore[T]) set(name string, obj *T) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return s._db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(s._bucketName).Put([]byte(name), data)
	})
}

var _ keyValueStore[initRecord] = (*recordStore[initRecord])(nil)
var _ keyValueStore[acquireRecord] = (*recordStore[acquireRecord])(nil)
