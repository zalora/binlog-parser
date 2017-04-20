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

type binlogParseFunc func(string, string) error

func createBinlogParseFunc(prettyPrint bool) binlogParseFunc {
	return func(binlogFilename string, outputDir string) error {
		return parseBinlogFile(binlogFilename, outputDir, prettyPrint)
	}
}

func parseBinlogFile(binlogFilename string, outputDir string, prettyPrint bool) error {
	glog.V(1).Infof("Parsing binlog file %s, output dir %s", binlogFilename, outputDir)

	if _, err := os.Stat(binlogFilename); os.IsNotExist(err) {
		return err
	}

	outputFile, err := os.Create(outputFilename(outputDir, binlogFilename))

	if err != nil {
		return err
	}

	defer outputFile.Close()

	glog.V(1).Info("About to connect to DB ...")

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
	consumerChain := parser.NewConsumerChain()

	consumerChain.CollectAsJsonInFile(outputFile, prettyPrint)

	glog.V(1).Info("About to parse file ...")

	return parser.ParseBinlog(binlogFilename, tableMap, consumerChain)
}

func outputFilename(outputDir string, binlogFilename string) string {
	basename := path.Base(binlogFilename)
	return path.Join(outputDir, fmt.Sprintf("%s.json", basename))
}
