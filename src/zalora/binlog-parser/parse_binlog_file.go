package main

import (
	"errors"
	"github.com/golang/glog"
	"os"
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

	return parser.ParseBinlog(binlogFilename, tableMap, consumerChain)
}
