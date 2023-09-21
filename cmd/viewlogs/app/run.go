package app

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

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

	table := newTablePrinter()

	if server {
		store, err := logs.NewInfoStore(db)
		if err != nil {
			return err
		}
		table.insertServerLogs(store.ServerRecord().Logs)
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
		table.insertInitLogs(resource, r.Logs)
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
		table.insertAcquisitionLogs(resource, r)
	}

	fmt.Printf("Resource identifier: %q\n\n", resource)
	table.print()

	return nil
}
