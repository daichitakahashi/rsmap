package logs

import (
	"encoding/json"
	"errors"

	"go.etcd.io/bbolt"
)

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
	RecordStore[T any] interface {
		Get(identifier string) (*T, error)
		Set(identifier string, record *T) error
		ForEach(fn func(identifier string, record *T) error) error
	}

	recordStore[T InitRecord | AcquireRecord] struct {
		_bucketName []byte
		_db         *bbolt.DB
	}
)

var ErrRecordNotFound = errors.New("record not found on key value store")

func NewRecordStore[T InitRecord | AcquireRecord](db *bbolt.DB) (RecordStore[T], error) {
	var (
		t          T
		v          any = t
		bucketName []byte
	)
	switch v.(type) {
	case InitRecord:
		bucketName = []byte("init")
	case AcquireRecord:
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

func (s *recordStore[T]) Set(identifier string, record *T) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s._db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(s._bucketName).Put([]byte(identifier), data)
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
