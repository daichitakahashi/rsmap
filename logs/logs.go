package logs

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
