package rsmap

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
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
		clientID string
		_cfg     config
		_mu      sync.RWMutex
		rm       resourceMap
		_stop    func()
	}

	// Resource brings an ability of acquire/release lock for the dedicated resource.
	Resource struct {
		_m    *Map
		_max  int64
		_name string
	}
)

type (
	NewOption struct {
		option.Interface
	}
	identOptionRetryPolicy struct{}
	identOptionHTTPClient  struct{}
)

// WithRetryPolicy specifies a retry policy of each operations(resource initializations, lock acquisitions).
// For example, interval, exponential-backoff and max retry.
// For detailed settings, see [backoff.NewExponentialPolicy] or [backoff.NewConstantPolicy].
func WithRetryPolicy(p backoff.Policy) *NewOption {
	return &NewOption{
		Interface: option.New(identOptionRetryPolicy{}, p),
	}
}

// WithHTTPClient specifies [http.Client] used for communication with server process.
// If your process launches server, this client may not be used.
func WithHTTPClient(c *http.Client) *NewOption {
	return &NewOption{
		Interface: option.New(identOptionHTTPClient{}, c),
	}
}

// New creates an instance of [Map] that enables us to reuse external resources with thread safety.
// Most common use-case is Go's parallelized testing of multiple packages (`go test -p=N ./...`.)
//
// Map has server mode and client mode.
// If Map has initialized in server mode, it creates database `${rsmapDir}/logs.db` and write server address to `${rsmapDir}/addr`.
// Other Map reads address of the server, and requests the server to acquire locks.
// So, every Go packages(directories) must specify same location as an argument. Otherwise, we cannot provide correct control.
// It's user's responsibility.
//
// In almost cases, following code can be helpful.
//
//	p,  _ := exec.Command("go", "mod", "GOMOD").Output() // Get file path of "go.mod".
//	m, _ := rsmap.New(filepath.Join(filepath.Dir(string(p)), ".rsmap"))
func New(rsmapDir string, opts ...*NewOption) (*Map, error) {
	// Create directory is not exists.
	if err := os.MkdirAll(rsmapDir, 0755); err != nil {
		return nil, fmt.Errorf("rsmap: failed to prepare directory(rsmapDir): %w", err)
	}

	cfg := config{
		dbFile:   filepath.Join(rsmapDir, "logs.db"),
		addrFile: filepath.Join(rsmapDir, "addr"),
		retryPolicy: backoff.NewConstantPolicy(
			backoff.WithMaxRetries(10),
			backoff.WithInterval(time.Millisecond*100),
		),
		httpCli: &http.Client{},
	}

	// Apply options.
	for _, opt := range opts {
		switch opt.Ident() {
		case identOptionRetryPolicy{}:
			cfg.retryPolicy = opt.Value().(backoff.Policy)
		case identOptionHTTPClient{}:
			cfg.httpCli = opt.Value().(*http.Client)
		}
	}

	m := &Map{
		clientID: uuid.NewString(),
		_cfg:     cfg,
		rm:       newClientSideMap(cfg),
	}

	// Start server launch process, and set release function.
	m._stop = m.launchServer()

	return m, nil
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

// WithMaxParallelism can specify max parallelism.
func WithMaxParallelism(n int64) *ResourceOption {
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
//
// Resource has a setting for max parallelism, you can specify the value by [WithMaxParallelism](default value is 5.)
// And you want to perform an initialization of the resource, use [WithInit].
func (m *Map) Resource(ctx context.Context, name string, opts ...*ResourceOption) (*Resource, error) {
	var (
		n    = int64(5)
		init InitFunc
	)

	// Apply options.
	for _, opt := range opts {
		switch opt.Ident() {
		case identOptionParallelism{}:
			n = opt.Value().(int64)
		case identOptionInit{}:
			init = opt.Value().(InitFunc)
		}
	}
	m._mu.RLock()
	try, err := m.rm.tryInit(ctx, name, m.clientID)
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
			err = m.rm.completeInit(ctx, name, m.clientID)
			m._mu.RUnlock()
			if err != nil {
				return nil, err
			}
		}
	}

	return &Resource{
		_m:   m,
		_max: n,
	}, nil
}

type ReleaseFunc func() error

// UseShared acquires shared lock of the Resource.
// Returned ReleaseFunc must be called after using the resource to release lock.
func (r *Resource) UseShared(ctx context.Context) (ReleaseFunc, error) {
	err := r._m.rm.acquire(ctx, r._name, r._m.clientID, r._max, false)
	if err != nil {
		return nil, err
	}
	return func() error {
		return r._m.rm.release(context.Background(), r._name, r._m.clientID)
	}, nil
}

// UseExclusive acquires exclusive lock of the Resource.
// Returned ReleaseFunc must be called after using the resource to release lock.
func (r *Resource) UseExclusive(ctx context.Context) (ReleaseFunc, error) {
	err := r._m.rm.acquire(ctx, r._name, r._m.clientID, r._max, true)
	if err != nil {
		return nil, err
	}
	return func() error {
		return r._m.rm.release(context.Background(), r._name, r._m.clientID)
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
