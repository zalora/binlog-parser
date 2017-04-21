package main

import (
	"errors"
	"fmt"
	"github.com/golang/glog"
	"os"
	"path"
	"zalora/binlog-parser/database"
	"zalora/binlog-parser/parser"
)

type binlogParseFunc func(string) error

func createBinlogParseFunc(consumerChain parser.ConsumerChain) binlogParseFunc {
	return func(binlogFilename string) error {
		return parseBinlogFile(binlogFilename, consumerChain)
	}
}

func parseBinlogFile(binlogFilename string, consumerChain parser.ConsumerChain) error {
	glog.V(2).Infof("Parsing binlog file %s", binlogFilename)

	db_dsn := os.Getenv("DB_DSN")

	if db_dsn == "" {
		return errors.New("Need env var DB_DSN to connect to MySQL instance")
	}

	db, err := database.GetDatabaseInstance(db_dsn)

	if err != nil {
		return err
	}

	defer db.Close()

	tableMap := database.NewTableMap(db)

	glog.V(2).Info("About to parse file ...")

	basename := path.Base(binlogFilename)
	err = consumerChain.CollectAsJsonInFile(fmt.Sprintf("%s.json", basename))

	if err != nil {
		return err
	}

	return parser.ParseBinlog(binlogFilename, tableMap, consumerChain)
}
