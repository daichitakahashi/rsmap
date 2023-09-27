package logs

import (
	"errors"

	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	logsv1 "github.com/daichitakahashi/rsmap/internal/proto/logs/v1"
)

var (
	bucketInfo    = []byte("info")
	bucketInit    = []byte("init")
	bucketAcquire = []byte("acquire")

	infoServerKey = []byte("server")
)

type InfoStore struct {
	_db *bbolt.DB

	_server *logsv1.ServerRecord
}

func NewInfoStore(db *bbolt.DB) (*InfoStore, error) {
	var server logsv1.ServerRecord

	err := db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketInfo)
		if err != nil {
			return err
		}

		data := b.Get(infoServerKey)
		if data != nil {
			err = proto.Unmarshal(data, &server)
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

func (s *InfoStore) ServerRecord() *logsv1.ServerRecord {
	return s._server
}

func (s *InfoStore) PutServerLog(l *logsv1.ServerLog) error {
	return s._db.Update(func(tx *bbolt.Tx) error {
		s._server.Logs = append(s._server.Logs, l)
		data, err := proto.Marshal(s._server)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketInfo).Put(infoServerKey, data)
	})
}

type (
	ResourceRecordStore[T any] interface {
		Get(identifier string) (*T, error)
		Put(identifier string, update func(r *T, update bool)) error
		ForEach(fn func(identifier string, record *T) error) error
	}

	ptr[L logsv1.InitRecord | logsv1.AcquisitionRecord] interface {
		*L
		protoreflect.ProtoMessage
	}

	recordStore[T logsv1.InitRecord | logsv1.AcquisitionRecord, P ptr[T]] struct {
		_bucketName []byte
		_db         *bbolt.DB
	}
)

var ErrRecordNotFound = errors.New("record not found on key value store")

func NewResourceRecordStore[T logsv1.InitRecord | logsv1.AcquisitionRecord, P ptr[T]](db *bbolt.DB) (ResourceRecordStore[T], error) {
	var (
		p          P                         = new(T)
		v          protoreflect.ProtoMessage = p
		bucketName []byte
	)
	switch v.(type) {
	case *logsv1.InitRecord:
		bucketName = bucketInit
	case *logsv1.AcquisitionRecord:
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
	return &recordStore[T, P]{
		_bucketName: bucketName,
		_db:         db,
	}, err
}

func (s *recordStore[T, P]) Get(identifier string) (*T, error) {
	var r P = new(T)
	err := s._db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(s._bucketName).Get([]byte(identifier))
		if data == nil {
			return ErrRecordNotFound
		}
		return proto.Unmarshal(data, r)
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s *recordStore[T, P]) Put(identifier string, fn func(r *T, update bool)) error {
	return s._db.Update(func(tx *bbolt.Tx) error {
		var (
			b   = tx.Bucket(s._bucketName)
			key = []byte(identifier)
		)

		var (
			r      P = new(T)
			update bool
		)
		data := b.Get(key)
		if data != nil {
			update = true
			if err := proto.Unmarshal(data, r); err != nil {
				return err
			}
		}

		fn(r, update)
		newData, err := proto.Marshal(r)
		if err != nil {
			return err
		}
		return b.Put(key, newData)
	})
}

func (s *recordStore[T, P]) ForEach(fn func(identifier string, record *T) error) error {
	return s._db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(s._bucketName).ForEach(func(k, v []byte) error {
			var r P = new(T)
			err := proto.Unmarshal(v, r)
			if err != nil {
				return err
			}
			return fn(string(k), r)
		})
	})
}
