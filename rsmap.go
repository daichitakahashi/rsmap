package rsmap

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/lestrrat-go/option"
	"go.etcd.io/bbolt"

	"github.com/daichitakahashi/rsmap/logs"
)

type (
	// Map is the controller for external resource usage.
	Map struct {
		_callers logs.CallerContext
		_cfg     config
		_mu      sync.RWMutex
		rm       resourceMap
		_stop    func()
	}

	// Resource brings an ability of acquire/release lock for the dedicated resource.
	Resource struct {
		_callers logs.CallerContext
		_m       *Map
		_max     int64
		_name    string
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

const (
	EnvExecutionID = "RSMAP_EXECUTION_ID"
)

func logsDir(base string) (string, error) {
	if !filepath.IsAbs(base) {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		base = filepath.Join(wd, base)
	}
	// Get execution ID.
	executionID := strconv.Itoa(os.Getppid()) // The process of `go test`
	if id, ok := os.LookupEnv(EnvExecutionID); ok {
		executionID = id
	}

	return filepath.Join(base, executionID), nil
}

// New creates an instance of [Map] that enables us to reuse external resources with thread safety.
// Most common use-case is Go's parallelized testing of multiple packages (`go test -p=N ./...`.)
//
// Map has server mode and client mode.
// If Map has initialized in server mode, it creates database `logs.db` and write server address to `addr` under `${rsmapDir}/${executionID}/`.
// Other Map reads address of the server, and requests the server to acquire locks.
// So, every Go packages(directories) must specify same location as an argument. Otherwise, we cannot provide correct control.
// It's user's responsibility.
//
// `executionID` is the identifier of the execution of `go test`. In default, we use the value of [os.Getppid()].
// If you want to specify the id explicitly, set the value to `RSMAP_EXECUTION_ID` environment variable.
//
// In almost cases, following code can be helpful.
//
//	p,  _ := exec.Command("go", "mod", "GOMOD").Output() // Get file path of "go.mod".
//	m, _ := rsmap.New(filepath.Join(filepath.Dir(strings.TrimSpace(string(p))), ".rsmap"))
func New(rsmapDir string, opts ...*NewOption) (*Map, error) {
	_, file, line, _ := runtime.Caller(1)
	var callers logs.CallerContext
	callers = callers.Append(file, line)

	// Get directory for `logs.db` and `addr`.
	dir, err := logsDir(rsmapDir)
	if err != nil {
		return nil, err
	}

	// Create directory if not exists.
	if err = os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("rsmap: failed to prepare directory(rsmapDir): %w", err)
	}

	cfg := config{
		dbFile:   filepath.Join(dir, "logs.db"),
		addrFile: filepath.Join(dir, "addr"),
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
		_callers: callers,
		_cfg:     cfg,
		rm:       newClientSideMap(cfg),
	}

	// Start server launch process, and set release function.
	m._stop = m.launchServer(dir, m._callers)

	return m, nil
}

func (m *Map) Close() {
	m._stop()
}

type (
	// ResourceOption represents option for [Resource]
	ResourceOption struct {
		option.Interface
	}
	identOptionParallelism struct{}
	identOptionInit        struct{}
)

// WithMaxParallelism specifies max parallelism of the resource usage.
func WithMaxParallelism(n int64) *ResourceOption {
	return &ResourceOption{
		Interface: option.New(identOptionParallelism{}, n),
	}
}

type InitFunc func(ctx context.Context) error

// WithInit specifies InitFunc for resource initialization.
//
// InitFunc will be called only once globally, at first declaration by [Resource].
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
	_, file, line, _ := runtime.Caller(1)
	callers := m._callers.Append(file, line)

	var (
		n             = int64(5)
		init InitFunc = func(ctx context.Context) error {
			// Do nothing.
			return nil
		}
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
	rm := m.rm
	m._mu.RUnlock()
	try, err := rm.tryInit(ctx, name, callers)
	if err != nil {
		return nil, err
	}
	if try {
		// Initialization of the resource.
		err = func() (err error) {
			var notPanicked bool
			defer func() {
				m._mu.RLock()
				rm := m.rm
				m._mu.RUnlock()

				// If init succeeds, mark as complete.
				if notPanicked && err == nil {
					err = rm.completeInit(ctx, name, callers)
					return
				}

				// Mark as failed when error or panic occurred.
				// CAUTION: Do not recover panic to preserve stacktrace.
				err = errors.Join(
					err,
					rm.failInit(ctx, name, callers),
				)
			}()

			err = init(ctx)
			notPanicked = true
			return
		}()
		if err != nil {
			return nil, err
		}
	}

	return &Resource{
		_callers: callers,
		_m:       m,
		_max:     n,
		_name:    name,
	}, nil
}

// RLock acquires shared lock of the Resource.
// The instance of Resource can acquire only one lock.
// Consecutive acquisition without unlock doesn't fail, but do nothing.
//
// To release lock, use [UnlockAny].
func (r *Resource) RLock(ctx context.Context) error {
	return r._m.rm.acquire(ctx, r._name, r._callers, r._max, false)
}

// Lock acquires exclusive lock of the Resource.
// The instance of Resource can acquire only one lock.
// Consecutive acquisition without unlock doesn't fail, but do nothing.
//
// To release lock, use [UnlockAny].
func (r *Resource) Lock(ctx context.Context) error {
	return r._m.rm.acquire(ctx, r._name, r._callers, r._max, true)
}

// UnlockAny releases acquired shared/exclusive lock by the Resource.
func (r *Resource) UnlockAny() error {
	return r._m.rm.release(context.Background(), r._name, r._callers)
}

// Core interface for control operations for both server and client side.
type resourceMap interface {
	tryInit(ctx context.Context, resourceName string, operator logs.CallerContext) (bool, error)
	completeInit(ctx context.Context, resourceName string, operator logs.CallerContext) error
	failInit(ctx context.Context, resourceName string, operator logs.CallerContext) error
	acquire(ctx context.Context, resourceName string, operator logs.CallerContext, max int64, exclusive bool) error
	release(ctx context.Context, resourceName string, operator logs.CallerContext) error
}

type serverSideMap struct {
	_init    *initController
	_acquire *acquireController
}

// Create resourceMap for server side.
// This map reads and updates bbolt.DB directly.
func newServerSideMap(db *bbolt.DB) (*serverSideMap, error) {
	initRecordStore, err := logs.NewResourceRecordStore[logs.InitRecord](db)
	if err != nil {
		return nil, err
	}
	acquireRecordStore, err := logs.NewResourceRecordStore[logs.AcquireRecord](db)
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

func (m *serverSideMap) tryInit(ctx context.Context, resourceName string, operator logs.CallerContext) (bool, error) {
	return m._init.tryInit(ctx, resourceName, operator)
}

func (m *serverSideMap) completeInit(_ context.Context, resourceName string, operator logs.CallerContext) error {
	return m._init.complete(resourceName, operator)
}

func (m *serverSideMap) failInit(_ context.Context, resourceName string, operator logs.CallerContext) error {
	return m._init.fail(resourceName, operator)
}

func (m *serverSideMap) acquire(ctx context.Context, resourceName string, operator logs.CallerContext, max int64, exclusive bool) error {
	return m._acquire.acquire(ctx, resourceName, operator, max, exclusive)
}

func (m *serverSideMap) release(_ context.Context, resourceName string, operator logs.CallerContext) error {
	return m._acquire.release(resourceName, operator)
}

var _ resourceMap = (*serverSideMap)(nil)
