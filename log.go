package rsmap

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

type initOp string

const (
	opStart    initOp = "start"
	opFail     initOp = "fail"
	opComplete initOp = "complete"
)

type initStatus string

const (
	statusStarted    initStatus = "started"
	statusProcessing initStatus = "processing"
	statusFailed     initStatus = "failed"
	statusCompleted  initStatus = "completed"
)

// acquireOp is a kind of operation recorded in acquire log.
type acquireOp string

const (
	opRequest acquireOp = "request"
	opAcquire acquireOp = "acquire"
)

type acquireStatus string

const (
	// statusRequested represents that the operation initiated an acquisition.
	statusRequested acquireStatus = "requested"
	// statusAcquiring represents that the acquisition operation is already initiated but not acquired yet.
	statusAcquiring acquireStatus = "acquiring"
	// statusAcquired represents that the acquisition operation is already completed.
	statusAcquired acquireStatus = "acquired"
)

// logs stores logs of the dedicated resource in memory.
// And all operation update the persisted datastore(BoltDB).
// This object is not concurrency-safe. All operations are intended to be used in the transaction.
type logs struct {
	info struct {
		max  int64
		data []byte
	}
	initLogs        []initLog
	acquisitionLogs map[string][]acquireLog // key=acquisitionID
}

type initLog struct {
	op initOp
	ts time.Time
}

func (l *logs) tryInit(tx *bbolt.Tx, ts time.Time, max int64) initStatus {
	st := statusStarted
	if ln := len(l.initLogs); ln > 0 {
		w := l.initLogs[ln-1]
		switch w.op {
		case opStart:
			st = statusProcessing
		case opFail:
			return statusFailed
		case opComplete:
			return statusCompleted
		}
	} else {
		l.info.max = max
	}
	l.initLogs = append(l.initLogs, initLog{
		op: opStart,
		ts: ts,
	})

	// TODO: store on BoltDB
	_ = tx

	return st
}

func (l *logs) completeInit(tx *bbolt.Tx, data any, ts time.Time) error {
	l.initLogs = append(l.initLogs, initLog{
		op: opComplete,
		ts: ts,
	})
	bs, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to serialize data: %w", err)
	}
	l.info.data = bs
	l.acquisitionLogs = map[string][]acquireLog{}

	// TODO: store on BoltDB
	_ = tx

	return nil
}

func (l *logs) failInit(tx *bbolt.Tx, ts time.Time) error {
	l.initLogs = append(l.initLogs, initLog{
		op: opFail,
		ts: ts,
	})

	// TODO: store on BoltDB
	_ = tx

	return nil
}

func (l *logs) initResult() (initStatus, bool) {
	if ln := len(l.initLogs); ln > 0 {
		switch l.initLogs[ln-1].op {
		case opStart:
			return statusStarted, true
		case opComplete:
			return statusCompleted, true
		case opFail:
			return statusFailed, true
		}
	}
	return "", false
}

type acquireLog struct {
	op     acquireOp
	ts     time.Time
	weight int64
}

func (l *logs) acquire(tx *bbolt.Tx, acquisitionID string, weight int64, ts time.Time) (acquireStatus, error) {
	if l.acquisitionLogs == nil {
		return "", errors.New("resource is not initialized")
	}

	ac, existing := l.acquisitionLogs[acquisitionID]
	st := statusRequested
	entry := acquireLog{
		op:     opRequest,
		ts:     ts,
		weight: weight,
	}
	if len(ac) > 0 {
		log := ac[len(ac)-1]
		switch log.op {
		case opRequest:
			st = statusAcquiring
		case opAcquire:
			st = statusAcquired
			entry.op = opAcquire
		}
		entry.weight = log.weight
	}

	if existing {
		l.acquisitionLogs[acquisitionID] = append(ac, entry)
	} else {
		l.acquisitionLogs[acquisitionID] = []acquireLog{entry}
	}

	// TODO: store on BoltDB
	_ = tx

	return st, nil
}

func (l *logs) completeAcquire(tx *bbolt.Tx, acquisitionID string, ts time.Time) error {
	if l.acquisitionLogs == nil {
		return errors.New("resource is not initialized")
	}

	ac := l.acquisitionLogs[acquisitionID]
	l.acquisitionLogs[acquisitionID] = append(ac, acquireLog{
		op: opAcquire,
		ts: ts, // required? or not?
	})

	// TODO: store on BoltDB
	_ = tx

	return nil
}

func (l *logs) release(tx *bbolt.Tx, acquisitionID string) error {
	if l.acquisitionLogs == nil {
		return errors.New("resource is not initialized")
	}

	delete(l.acquisitionLogs, acquisitionID)

	// TODO: remove from BoltDB
	_ = tx

	return nil
}
