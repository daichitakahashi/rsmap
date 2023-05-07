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

	"github.com/google/uuid"
	"github.com/lestrrat-go/backoff/v2"
	"go.etcd.io/bbolt"
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

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}

	var s http.Server
	m := http.NewServeMux()
	m.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req acquireRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(acquireResponse{
			ID: req.ID,
		})
	}))
	s.Handler = m
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

type acquireRequest struct {
	ClientID string `json:"clientId"`
	ID       string `json:"id"`
}

type acquireResponse struct {
	ID string `json:"id"`
}
