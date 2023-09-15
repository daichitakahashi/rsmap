package rsmap

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"time"

	connect_go "github.com/bufbuild/connect-go"
	"github.com/lestrrat-go/backoff/v2"
	"go.etcd.io/bbolt"

	resource_mapv1 "github.com/daichitakahashi/rsmap/internal/proto/resource_map/v1"
	"github.com/daichitakahashi/rsmap/internal/proto/resource_map/v1/resource_mapv1connect"
	"github.com/daichitakahashi/rsmap/logs"
)

type config struct {
	dbFile      string
	addrFile    string
	retryPolicy backoff.Policy
	httpCli     *http.Client
}

// Open database for server.
// This blocks indefinitely.
func (c *config) openDB() (*bbolt.DB, error) {
	return bbolt.Open(c.dbFile, 0644, nil) // Set options if required.
}

// Read server address.
func (c *config) readAddr() (string, error) {
	data, err := os.ReadFile(c.addrFile)
	if err != nil {
		return "", err
	}
	return string(bytes.TrimSpace(data)), nil
}

// Write server address for other clients.
func (c *config) writeAddr(addr string) error {
	return os.WriteFile(c.addrFile, []byte(addr), 0644)
}

func (m *Map) launchServer(clientID string) func() {
	done := make(chan struct{})

	go func() {
		db, err := m._cfg.openDB()
		if err != nil {
			return
		}
		defer db.Close()

		select {
		case <-done:
			return
		default:
		}

		info, err := logs.NewInfoStore(db)
		if err != nil {
			return
		}

		rm, err := newServerSideMap(db)
		if err != nil {
			return
		}

		// Launch server.
		ln, err := net.Listen("tcp", ":0")
		if err != nil {
			return
		}
		mux := http.NewServeMux()
		mux.Handle(
			resource_mapv1connect.NewResourceMapServiceHandler(&resourceMapHandler{
				_rm: rm,
			}),
		)
		s := http.Server{
			Handler: mux,
		}
		go func() {
			_ = s.Serve(ln)
		}()

		// Write addr for other clients.
		addr := "http://" + ln.Addr().String()
		err = m._cfg.writeAddr(addr)
		if err != nil {
			return
		}

		// Record launched server.
		err = info.PutServerLog(logs.ServerLog{
			Event:     logs.ServerEventLaunched,
			Addr:      addr,
			Operator:  clientID,
			Timestamp: time.Now().UnixNano(),
		})
		if err != nil {
			return
		}

		// Replace resourceMap with serverSideMap.
		m._mu.Lock()
		m.rm = rm
		m._mu.Unlock()

		<-done
		_ = s.Shutdown(context.Background())

		// Record stopped server.
		_ = info.PutServerLog(logs.ServerLog{
			Event:     logs.ServerEventStopped,
			Operator:  clientID,
			Timestamp: time.Now().UnixNano(),
		})
	}()

	return func() {
		close(done)
	}
}

type resourceMapHandler struct {
	_rm *serverSideMap
}

func (h *resourceMapHandler) TryInitResource(ctx context.Context, req *connect_go.Request[resource_mapv1.TryInitResourceRequest]) (*connect_go.Response[resource_mapv1.TryInitResourceResponse], error) {
	try, err := h._rm.tryInit(ctx, req.Msg.ResourceName, req.Msg.ClientId)
	if err != nil {
		return nil, err
	}
	return connect_go.NewResponse(&resource_mapv1.TryInitResourceResponse{
		ShouldTry: try,
	}), nil
}

func (h *resourceMapHandler) CompleteInitResource(ctx context.Context, req *connect_go.Request[resource_mapv1.CompleteInitResourceRequest]) (*connect_go.Response[resource_mapv1.CompleteInitResourceResponse], error) {
	err := h._rm.completeInit(ctx, req.Msg.ResourceName, req.Msg.ClientId)
	if err != nil {
		return nil, err
	}
	return connect_go.NewResponse(&resource_mapv1.CompleteInitResourceResponse{}), nil
}

func (h *resourceMapHandler) Acquire(ctx context.Context, req *connect_go.Request[resource_mapv1.AcquireRequest]) (*connect_go.Response[resource_mapv1.AcquireResponse], error) {
	err := h._rm.acquire(ctx, req.Msg.ResourceName, req.Msg.ClientId, req.Msg.MaxParallelism, req.Msg.Exclusive)
	if err != nil {
		return nil, err
	}
	return connect_go.NewResponse(&resource_mapv1.AcquireResponse{}), nil
}

func (h *resourceMapHandler) Release(ctx context.Context, req *connect_go.Request[resource_mapv1.ReleaseRequest]) (*connect_go.Response[resource_mapv1.ReleaseResponse], error) {
	err := h._rm.release(ctx, req.Msg.ResourceName, req.Msg.ClientId)
	if err != nil {
		return nil, err
	}
	return connect_go.NewResponse(&resource_mapv1.ReleaseResponse{}), nil
}

var _ resource_mapv1connect.ResourceMapServiceHandler = (*resourceMapHandler)(nil)

type clientSideMap struct {
	_cfg config
}

func newClientSideMap(cfg config) *clientSideMap {
	return &clientSideMap{
		_cfg: cfg,
	}
}

func (m *clientSideMap) try(ctx context.Context, op func(ctx context.Context, cli resource_mapv1connect.ResourceMapServiceClient) error) error {
	var (
		addr string
		err  error
		ctl  = m._cfg.retryPolicy.Start(ctx)
	)
	for {
		select {
		case <-ctl.Done():
			// When ctx is canceled, or retry count is exceeded.
			if e := ctx.Err(); e != nil {
				return e
			}
			return err
		case <-ctl.Next():
			addr, err = m._cfg.readAddr()
			if err != nil {
				// Retry!
				continue
			}
			// MEMO: Do we need to reuse service clients?
			cli := resource_mapv1connect.NewResourceMapServiceClient(m._cfg.httpCli, addr)
			if err = op(ctx, cli); err != nil {
				// Retry!
				continue
			}
			return nil
		}
	}
}

func (m *clientSideMap) tryInit(ctx context.Context, resourceName string, operator string) (try bool, _ error) {
	err := m.try(ctx, func(ctx context.Context, cli resource_mapv1connect.ResourceMapServiceClient) error {

		resp, err := cli.TryInitResource(ctx, connect_go.NewRequest(&resource_mapv1.TryInitResourceRequest{
			ResourceName: resourceName,
			ClientId:     operator,
		}))
		if err != nil {
			return err
		}

		try = resp.Msg.ShouldTry
		return nil
	})
	return try, err
}

func (m *clientSideMap) completeInit(ctx context.Context, resourceName string, operator string) error {
	return m.try(ctx, func(ctx context.Context, cli resource_mapv1connect.ResourceMapServiceClient) error {

		_, err := cli.CompleteInitResource(ctx, connect_go.NewRequest(&resource_mapv1.CompleteInitResourceRequest{
			ResourceName: resourceName,
			ClientId:     operator,
		}))

		return err
	})
}

func (m *clientSideMap) acquire(ctx context.Context, resourceName string, operator string, max int64, exclusive bool) error {
	return m.try(ctx, func(ctx context.Context, cli resource_mapv1connect.ResourceMapServiceClient) error {

		_, err := cli.Acquire(ctx, connect_go.NewRequest(&resource_mapv1.AcquireRequest{
			ResourceName:   resourceName,
			ClientId:       operator,
			MaxParallelism: max,
			Exclusive:      exclusive,
		}))

		return err
	})
}

func (m *clientSideMap) release(ctx context.Context, resourceName string, operator string) error {
	return m.try(ctx, func(ctx context.Context, cli resource_mapv1connect.ResourceMapServiceClient) error {

		_, err := cli.Release(ctx, connect_go.NewRequest(&resource_mapv1.ReleaseRequest{
			ResourceName: resourceName,
			ClientId:     operator,
		}))

		return err
	})
}

var _ resourceMap = (*clientSideMap)(nil)
