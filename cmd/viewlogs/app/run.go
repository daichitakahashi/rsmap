package app

import (
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/rodaine/table"
	"github.com/spf13/pflag"
	"go.etcd.io/bbolt"

	logsv1 "github.com/daichitakahashi/rsmap/internal/proto/logs/v1"
	"github.com/daichitakahashi/rsmap/logs"
)

func Run() {
	var (
		operation = pflag.StringP("operation", "o", "", "")
		resource  = pflag.StringP("resource", "r", "", "")
	)
	pflag.Parse()

	filename := pflag.Arg(0)
	if filename == "" {
		log.Fatal("logs.db file must be specified")
	}

	if err := run(filename, *operation, *resource); err != nil {
		log.Fatal(err)
	}
}

func run(filename, operation, resource string) error {
	_, err := os.Stat(filename)
	if err != nil {
		return err
	}

	db, err := bbolt.Open(filename, 0644, nil)
	if err != nil {
		return fmt.Errorf("failed to open database: %s", err)
	}

	var server, init, acquire bool
	if operation == "" {
		server = true
		init = true
		acquire = true
	} else {
		for _, op := range strings.Split(operation, ",") {
			switch op {
			case "server":
				server = true
			case "init":
				init = true
			case "acquire":
				acquire = true
			}
		}
	}

	if resource == "" {
		// TODO: add full list option?
		return errors.New("resource id must be specified")
	}

	type row struct {
		ts        int64 // timestamp
		operation string
		context   string
		data      string
	}
	var rows []row
	insert := func(r row) {
		idx, _ := slices.BinarySearchFunc(rows, r, func(r1, r2 row) int {
			switch {
			case r1.ts == r2.ts:
				return 0
			case r1.ts < r2.ts:
				return -1
			default:
				return 1
			}
		})

		if idx == len(rows) {
			rows = append(rows, r)
		} else {
			rows = append(rows, row{})
			copy(rows[idx+1:], rows[idx:])
			rows[idx] = r
		}
	}

	if server {
		store, err := logs.NewInfoStore(db)
		if err != nil {
			return err
		}

		for _, l := range store.ServerRecord().Logs {
			var data string
			if l.Event == logsv1.ServerEvent_SERVER_EVENT_LAUNCHED {
				data = fmt.Sprintf("addr=%s", l.Addr)
			}
			insert(row{
				ts:        l.Timestamp,
				operation: formatServerOperation(l.Event),
				context:   logs.CallerContext(l.Context).String(),
				data:      data,
			})
		}
	}

	if init {
		store, err := logs.NewResourceRecordStore[logsv1.InitRecord](db)
		if err != nil {
			return err
		}

		r, err := store.Get(resource)
		if err != nil {
			return err
		}
		for _, l := range r.Logs {
			insert(row{
				ts:        l.Timestamp,
				operation: formatInitOperation(l.Event),
				context:   logs.CallerContext(l.Context).String(),
				data:      "",
			})
		}
	}

	if acquire {
		store, err := logs.NewResourceRecordStore[logsv1.AcquisitionRecord](db)
		if err != nil {
			return err
		}

		r, err := store.Get(resource)
		if err != nil {
			return err
		}
		for i, l := range r.Logs {
			var data string
			if i == 0 {
				data = fmt.Sprintf("Max=%d,", r.Max)
			}
			if l.Event == logsv1.AcquisitionEvent_ACQUISITION_EVENT_ACQUIRED {
				data = fmt.Sprintf("%sN=%d", data, l.N)
			}
			insert(row{
				ts:        l.Timestamp,
				operation: formatAcquisitionOperation(l.Event),
				context:   logs.CallerContext(l.Context).String(),
				data:      data,
			})
		}
	}

	fmt.Printf("Resource identifier: %q\n\n", resource)
	tbl := table.New("Time", "Operation", "Context(Map->Resource)", "Data").
		WithHeaderFormatter(
			color.New(color.FgGreen, color.Underline).SprintfFunc(),
		).
		WithFirstColumnFormatter(
			color.New(color.FgYellow).SprintfFunc(),
		)

	var last time.Time
	for _, r := range rows {
		tbl.AddRow(formatTime(r.ts, &last), r.operation, r.context, r.data)
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

func formatTime(ts int64, last *time.Time) string {
	t := time.Unix(0, ts)
	defer func() {
		*last = t
	}()
	s := t.Format("2006-01-02 15:04:05.999999999")
	if last.IsZero() {
		return s
	}
	diff := t.Sub(*last)
	if diff >= 0 {
		return fmt.Sprintf("%s(+%s)", s, diff)
	}
	return fmt.Sprintf("%s(%s)", s, diff)
}
