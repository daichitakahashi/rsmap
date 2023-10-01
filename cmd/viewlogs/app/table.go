package app

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"time"

	"github.com/fatih/color"
	"github.com/rodaine/table"
	"golang.org/x/mod/modfile"

	logsv1 "github.com/daichitakahashi/rsmap/internal/proto/logs/v1"
	"github.com/daichitakahashi/rsmap/logs"
)

type (
	row struct {
		ts        int64 // timestamp
		resource  string
		operation string
		data      string
		context   logs.CallerContext
	}

	tablePrinter struct {
		singleResource bool
		shortContext   bool

		rows []row
	}
)

func newTablePrinter(singleResource, shortContext bool) *tablePrinter {
	return &tablePrinter{
		singleResource: singleResource,
		shortContext:   shortContext,

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
			data = l.Addr
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
	for _, l := range il {
		p.insert(row{
			ts:        l.Timestamp,
			resource:  resource,
			operation: formatInitOperation(l.Event),
			context:   logs.CallerContext(l.Context),
		})
	}
}

func (p *tablePrinter) insertAcquisitionLogs(resource string, r *logsv1.AcquisitionRecord) {
	var (
		acquiring = map[string]int64{}
		acquired  = map[string]int64{}
		total     int64
	)

	for _, l := range r.Logs {
		var (
			data string
			cc   = logs.CallerContext(l.Context).ShortString()
		)
		switch l.Event {
		case logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRING:
			if _, ok := acquiring[cc]; !ok {
				acquiring[cc] = l.Timestamp
			}
			continue
		case logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED:
			var elapsed string
			start, ok := acquiring[cc]
			if ok {
				delete(acquiring, cc)
				elapsed = fmt.Sprintf(" [waited %s]", time.Duration(l.Timestamp-start))
			}

			total += l.N
			acquired[cc] = l.N
			data = fmt.Sprintf("+%d(%d/%d)%s", l.N, total, r.Max, elapsed)
		case logsv1.AcquisitionEvent_ACQUISITION_EVENT_RELEASED:
			if n, ok := acquired[cc]; ok {
				total -= n
				delete(acquired, cc)
				data = fmt.Sprintf("-%d(%d/%d)", n, total, r.Max)
			}
		}
		p.insert(row{
			ts:        l.Timestamp,
			resource:  resource,
			operation: formatAcquisitionOperation(l.Event),
			context:   logs.CallerContext(l.Context),
			data:      data,
		})
	}
}

func (p *tablePrinter) print() error {
	pathShortener, err := newPathShortener()
	if err != nil {
		return err
	}

	var tbl table.Table
	if p.singleResource {
		tbl = table.New("Time", "Elapsed", "Operation", "Data", "Context(Map->Resource)")
	} else {
		tbl = table.New("Time", "Resource", "Operation", "Data", "Context(Map->Resource)")
	}

	tbl = tbl.
		WithHeaderFormatter(
			color.New(color.FgGreen, color.Underline).SprintfFunc(),
		).
		WithFirstColumnFormatter(
			color.New(color.FgYellow).SprintfFunc(),
		)

	var last time.Time
	for _, r := range p.rows {
		timestamp, elapsed := formatTime(r.ts, &last)

		// Shorten context filepaths.
		for _, c := range r.context {
			if err := pathShortener.shorten(c); err != nil {
				return err
			}
		}
		var ctx string
		if p.shortContext {
			ctx = r.context.ShortString()
		} else {
			ctx = r.context.String()
		}

		if p.singleResource {
			tbl.AddRow(timestamp, elapsed, r.operation, r.data, ctx)
		} else {
			tbl.AddRow(timestamp, r.resource, r.operation, r.data, ctx)
		}
	}

	tbl.Print()
	return nil
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

type pathShortener struct {
	wd      string
	modName string

	isModulePath struct { // go test -trimpath
		checked bool
		value   bool
	}
}

func newPathShortener() (*pathShortener, error) {
	// Get current directory for context resolution.
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	// Get module name of "wd" for context resolution.
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		return nil, err
	}
	path := string(bytes.TrimSpace(out))
	if path == "" || path == os.DevNull {
		return &pathShortener{
			wd: wd,
		}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	mod, err := modfile.Parse("go.mod", data, nil)
	if err != nil {
		return nil, err
	}

	return &pathShortener{
		wd:      wd,
		modName: mod.Module.Mod.Path,
	}, nil
}

func (s *pathShortener) shorten(c *logsv1.Caller) error {
	if !s.isModulePath.checked {
		s.isModulePath.value = !filepath.IsAbs(c.File)
		s.isModulePath.checked = true
	}

	var err error
	if s.isModulePath.value {
		c.File, err = filepath.Rel(s.modName, c.File)
	} else {
		c.File, err = filepath.Rel(s.wd, c.File)
	}
	return err
}
