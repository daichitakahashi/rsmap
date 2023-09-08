package rsmap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	connect_go "github.com/bufbuild/connect-go"
	"github.com/google/uuid"
	"github.com/lestrrat-go/backoff/v2"
	"go.etcd.io/bbolt"

	resource_mapv1 "github.com/daichitakahashi/rsmap/internal/proto/resource_map/v1"
	"github.com/daichitakahashi/rsmap/internal/proto/resource_map/v1/resource_mapv1connect"
)

func connect(ctx context.Context, retry int) (func(), error) {
	c := backoff.NewConstantPolicy(
		backoff.WithInterval(time.Millisecond*500),
		backoff.WithMaxRetries(retry),
	).Start(ctx)
	for {
		select {
		case <-c.Done():
			return nil, errors.New("failed to connect server or establish new server")
		case <-c.Next():
			addr, err := readAddr()
			if err == nil {
				err = request(addr)
				if err == nil {
					return func() {}, nil
				}
			}
			closeFn, err := newServer()
			if err == nil {
				return closeFn, nil
			}
		}
	}
}

func readAddr() (string, error) {
	data, err := os.ReadFile("addr")
	if err != nil {
		return "", err
	}
	return string(bytes.TrimSpace(data)), nil
}

func writeAddr(addr string) error {
	return os.WriteFile("addr", []byte(addr), 0644)
}

func request(addr string) error {
	var c http.Client
	c.Timeout = time.Millisecond * 50

	req := acquireRequest{
		ClientID: uuid.NewString(),
		ID:       uuid.NewString(),
	}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	resp, err := c.Post("http://"+addr, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	var res acquireResponse
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return err
	}
	if res.ID != req.ID {
		return errors.New("unexpected id")
	}
	return nil
}

func newServer() (_ func(), err error) {
	db, err := bbolt.Open("db.db", 0644, &bbolt.Options{
		Timeout: time.Millisecond * 50,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()

	rm, err := newServerSideMap(db)
	if err != nil {
		return nil, err
	}

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}

	http.Handle(
		resource_mapv1connect.NewResourceMapServiceHandler(&resourceMapHandler{
			_rm: rm,
		}),
	)
	s := http.Server{
		Handler: http.DefaultServeMux,
	}
	go func() {
		err := s.Serve(ln)
		_ = err // TODO:
	}()

	err = writeAddr(ln.Addr().String())
	if err != nil {
		return nil, errors.Join(err, ln.Close())
	}

	return func() {
		_ = db.Close()
		_ = s.Close()
	}, nil
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

type acquireRequest struct {
	ClientID string `json:"clientId"`
	ID       string `json:"id"`
}

type acquireResponse struct {
	ID string `json:"id"`
}
