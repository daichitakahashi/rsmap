package app

import (
	"fmt"
	"slices"
	"time"

	"github.com/fatih/color"
	"github.com/rodaine/table"

	logsv1 "github.com/daichitakahashi/rsmap/internal/proto/logs/v1"
	"github.com/daichitakahashi/rsmap/logs"
)

type (
	row struct {
		ts        int64 // timestamp
		operation string
		data      string
		context   logs.CallerContext
	}

	tablePrinter struct {
		shortContext bool

		rows []row
	}
)

func newTablePrinter(shortContext bool) *tablePrinter {
	return &tablePrinter{
		shortContext: shortContext,

		rows: []row{},
	}
}

func (p *tablePrinter) insert(v row) {
	idx, _ := slices.BinarySearchFunc(p.rows, v, func(r1, r2 row) int {
		switch {
		case r1.ts == r2.ts:
			return 0
		case r1.ts < r2.ts:
			return -1
		default:
			return 1
		}
	})

	if idx == len(p.rows) {
		p.rows = append(p.rows, v)
	} else {
		p.rows = append(p.rows, row{})
		copy(p.rows[idx+1:], p.rows[idx:])
		p.rows[idx] = v
	}
}

func (p *tablePrinter) insertServerLogs(sl []*logsv1.ServerLog) {
	for _, l := range sl {
		var data string
		if l.Event == logsv1.ServerEvent_SERVER_EVENT_LAUNCHED {
			data = fmt.Sprintf("addr=%s", l.Addr)
		}
		p.insert(row{
			ts:        l.Timestamp,
			operation: formatServerOperation(l.Event),
			data:      data,
			context:   logs.CallerContext(l.Context),
		})
	}
}

func (p *tablePrinter) insertInitLogs(resource string, il []*logsv1.InitLog) {
	_ = resource

	for _, l := range il {
		p.insert(row{
			ts:        l.Timestamp,
			operation: formatInitOperation(l.Event),
			context:   logs.CallerContext(l.Context),
		})
	}
}

func (p *tablePrinter) insertAcquisitionLogs(resource string, r *logsv1.AcquisitionRecord) {
	_ = resource

	for i, l := range r.Logs {
		var data string
		if i == 0 {
			data = fmt.Sprintf("Max=%d,", r.Max)
		}
		if l.Event == logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED {
			data = fmt.Sprintf("%sN=%d", data, l.N)
		}
		p.insert(row{
			ts:        l.Timestamp,
			operation: formatAcquisitionOperation(l.Event),
			context:   logs.CallerContext(l.Context),
			data:      data,
		})
	}
}

func (p *tablePrinter) print() {
	tbl := table.New("Time", "Elapsed", "Operation", "Data", "Context(Map->Resource)").
		WithHeaderFormatter(
			color.New(color.FgGreen, color.Underline).SprintfFunc(),
		).
		WithFirstColumnFormatter(
			color.New(color.FgYellow).SprintfFunc(),
		)

	var last time.Time
	for _, r := range p.rows {
		timestamp, elapsed := formatTime(r.ts, &last)
		var ctx string
		if p.shortContext {
			ctx = r.context.ShortString()
		} else {
			ctx = r.context.String()
		}
		tbl.AddRow(timestamp, elapsed, r.operation, r.data, ctx)
	}
	tbl.Print()
}

func formatServerOperation(e logsv1.ServerEvent) string {
	switch e {
	case logsv1.ServerEvent_SERVER_EVENT_LAUNCHED:
		return "server:launched"
	case logsv1.ServerEvent_SERVER_EVENT_STOPPED:
		return "server:stopped"
	default:
		return e.String()
	}
}

func formatInitOperation(e logsv1.InitEvent) string {
	switch e {
	case logsv1.InitEvent_INIT_EVENT_STARTED:
		return "init:started"
	case logsv1.InitEvent_INIT_EVENT_COMPLETED:
		return "init:completed"
	case logsv1.InitEvent_INIT_EVENT_FAILED:
		return "init:failed"
	default:
		return e.String()
	}
}

func formatAcquisitionOperation(e logsv1.AcquisitionEvent) string {
	switch e {
	case logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED:
		return "acquired"
	case logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED:
		return "released"
	default:
		return e.String()
	}
}

func formatTime(ts int64, last *time.Time) (string, string) {
	t := time.Unix(0, ts)
	defer func() {
		*last = t
	}()
	s := t.Format("2006-01-02 15:04:05.999999999")
	if last.IsZero() {
		return s, ""
	}
	diff := t.Sub(*last)
	if diff >= 0 {
		return s, "+" + diff.String()
	}
	return s, diff.String()
}
