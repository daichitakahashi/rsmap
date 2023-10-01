package app

import (
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
		operation    = pflag.StringP("operation", "o", "", "")
		resource     = pflag.StringP("resource", "r", "", "")
		shortContext = pflag.BoolP("short", "s", false, "")
	)
	pflag.Parse()

	filename := pflag.Arg(0)
	if filename == "" {
		log.Fatal("logs.db file must be specified")
	}

	if err := run(filename, *operation, *resource, *shortContext); err != nil {
		log.Fatal(err)
	}
}

func run(filename, operation, resource string, shortContext bool) error {
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

	var resources []string
	if resource == "" {
		resources, err = allResources(db)
		if err != nil {
			return err
		}
	} else {
		resources = []string{resource}
	}

	table := newTablePrinter(len(resources) == 1, shortContext)
	if server {
		store, err := logs.NewInfoStore(db)
		if err != nil {
			return err
		}
		table.insertServerLogs(store.ServerRecord().Logs)
	}

	for _, resource := range resources {

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
	}

	return table.print()
}

func allResources(db *bbolt.DB) ([]string, error) {
	var resources []string

	err := db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket([]byte("init")).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			resources = append(resources, string(k))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resources, nil
}
