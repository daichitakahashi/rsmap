package logs

import (
	"encoding/json"
	"errors"

	"go.etcd.io/bbolt"
)

var (
	bucketInfo    = []byte("info")
	bucketInit    = []byte("init")
	bucketAcquire = []byte("acquire")

	infoServerKey = []byte("server")
)

type ServerEvent string

const (
	ServerEventLaunched = "launched"
	ServerEventStopped  = "stopped"
)

type (
	ServerRecord struct {
		Logs []ServerLog
	}

	ServerLog struct {
		Event     ServerEvent `json:"event"`
		Addr      string      `json:"addr,omitempty"`
		Operator  string      `json:"operator"`
		Timestamp int64       `json:"ts,string"`
	}
)

type InfoStore struct {
	_db *bbolt.DB

	_server *ServerRecord
}

func NewInfoStore(db *bbolt.DB) (*InfoStore, error) {
	var server ServerRecord

	err := db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketInfo)
		if err != nil {
			return err
		}

		data := b.Get(infoServerKey)
		if data != nil {
			err = json.Unmarshal(data, &server)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &InfoStore{
		_db:     db,
		_server: &server,
	}, nil
}

func (s *InfoStore) ServerRecord() *ServerRecord {
	return s._server
}

func (s *InfoStore) PutServerLog(l ServerLog) error {
	return s._db.Update(func(tx *bbolt.Tx) error {
		s._server.Logs = append(s._server.Logs, l)
		data, err := json.Marshal(s._server)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketInfo).Put(infoServerKey, data)
	})
}

type InitEvent string

const (
	InitEventStarted   InitEvent = "started"
	InitEventCompleted InitEvent = "completed"
)

type (
	InitRecord struct {
		Logs []InitLog `json:"logs"`
	}

	InitLog struct {
		Event     InitEvent `json:"event"`
		Operator  string    `json:"operator"`
		Timestamp int64     `json:"ts,string"`
	}
)

type AcquireEvent string

const (
	AcquireEventAcquired AcquireEvent = "acquired"
	AcquireEventReleased AcquireEvent = "released"
)

type (
	AcquireRecord struct {
		Max  int64        `json:"max,string"`
		Logs []AcquireLog `json:"logs"`
	}

	AcquireLog struct {
		Event     AcquireEvent `json:"event"`
		N         int64        `json:"n,string,omitempty"`
		Operator  string       `json:"operator"`
		Timestamp int64        `json:"ts,string"`
	}
)

type (
	ResourceRecordStore[T any] interface {
		Get(identifier string) (*T, error)
		Put(identifier string, update func(r *T, update bool)) error
		ForEach(fn func(identifier string, record *T) error) error
	}

	recordStore[T InitRecord | AcquireRecord] struct {
		_bucketName []byte
		_db         *bbolt.DB
	}
)

var ErrRecordNotFound = errors.New("record not found on key value store")

func NewResourceRecordStore[T InitRecord | AcquireRecord](db *bbolt.DB) (ResourceRecordStore[T], error) {
	var (
		t          T
		v          any = t
		bucketName []byte
	)
	switch v.(type) {
	case InitRecord:
		bucketName = bucketInit
	case AcquireRecord:
		bucketName = bucketAcquire
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

func (s *recordStore[T]) Get(identifier string) (*T, error) {
	var r T
	err := s._db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(s._bucketName).Get([]byte(identifier))
		if data == nil {
			return ErrRecordNotFound
		}
		return json.Unmarshal(data, &r)
	})
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *recordStore[T]) Put(identifier string, fn func(r *T, update bool)) error {
	return s._db.Update(func(tx *bbolt.Tx) error {
		var (
			b   = tx.Bucket(s._bucketName)
			key = []byte(identifier)
		)

		var (
			r      T
			update bool
		)
		data := b.Get(key)
		if data != nil {
			update = true
			if err := json.Unmarshal(data, &r); err != nil {
				return err
			}
		}

		fn(&r, update)
		newData, err := json.Marshal(r)
		if err != nil {
			return err
		}
		return b.Put(key, newData)
	})
}

func (s *recordStore[T]) ForEach(fn func(identifier string, record *T) error) error {
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
