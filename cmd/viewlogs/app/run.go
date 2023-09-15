package app

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/rodaine/table"
	"github.com/spf13/pflag"
	"go.etcd.io/bbolt"

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

	var init, acquire bool
	switch operation {
	case "init":
		init = true
	case "acquire":
		acquire = true
	default:
		init = true
		acquire = true
	}

	if resource == "" {
		// TODO: add full list option?
		return errors.New("resource id must be specified")
	}

	tbl := table.New("Timestamp", "Operation", "Operator", "Data")
	var last time.Time

	if init {
		store, err := logs.NewResourceRecordStore[logs.InitRecord](db)
		if err != nil {
			return err
		}

		r, err := store.Get(resource)
		if err != nil {
			return err
		}
		for _, l := range r.Logs {
			tbl.AddRow(formatTime(l.Timestamp, &last), "init:"+l.Event, l.Operator, "")
		}
	}

	if acquire {
		store, err := logs.NewResourceRecordStore[logs.AcquireRecord](db)
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
			if l.Event == logs.AcquireEventAcquired {
				data = fmt.Sprintf("%sN=%d", data, l.N)
			}
			tbl.AddRow(formatTime(l.Timestamp, &last), l.Event, l.Operator, data)
		}
	}

	fmt.Printf("Resource identifier: %q\n\n", resource)
	tbl.
		WithHeaderFormatter(
			color.New(color.FgGreen, color.Underline).SprintfFunc(),
		).
		WithFirstColumnFormatter(
			color.New(color.FgYellow).SprintfFunc(),
		).Print()

	return nil
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
