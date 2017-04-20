package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"zalora/binlog-parser/database"
	"zalora/binlog-parser/parser"
	"zalora/binlog-parser/watcher"
)

// global flags
var modeFlag = flag.String("mode", "", "[parse|watch]")
var outputDirFlag = flag.String("output-dir", "/tmp", "Directory to dump output files to")

// parser flags
var binlogFilenameFlag = flag.String("file", "", "binlog file to parse")

// watcher flags
var binlogIndexFilenameFlag = flag.String("binlog-index", "", "binlog dir to watch")
var watcherIndexFilenameFlag = flag.String("watcher-index", "/tmp/watcher.index", "filename for indexer to keep state (will be created if it doesn't exist)")

func main() {
	flag.Parse()

	switch *modeFlag {
	case "parse":
		binlogFileName := *binlogFilenameFlag

		err := parse(binlogFileName)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Got error: %s", err)
			os.Exit(1)
		}

		break

	case "watch":
		binlogIndexFilename := *binlogIndexFilenameFlag

		if _, err := os.Stat(binlogIndexFilename); os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "binlog index file does not exist")
			os.Exit(1)
		}

		watcherIndexFilename := *watcherIndexFilenameFlag

		err := watcher.WatchBinlogIndexFile(
			binlogIndexFilename,
			watcherIndexFilename,
			parse,
		)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Got error: %s", err)
		}

		break

	default:
		fmt.Printf("Usage: %s [parse|watch]\n", os.Args[0])
		os.Exit(1)

		break
	}
}

func parse(binlogFileName string) error {
	outputDir := *outputDirFlag

	if _, err := os.Stat(binlogFileName); os.IsNotExist(err) {
		return err
	}

	outputFile, err := os.Create(outputFilename(outputDir, binlogFileName))

	if err != nil {
		return err
	}

	defer outputFile.Close()

	db, err := database.GetDatabaseInstance("root@/test_db")

	if err != nil {
		return err
	}

	defer db.Close()

	tableMap := database.NewTableMap(db)
	consumerChain := parser.NewConsumerChain()

	consumerChain.CollectAsJsonInFile(outputFile)

	return parser.ParseBinlog(binlogFileName, tableMap, consumerChain)
}

func getEnvOrDefault(varname string, defaultValue string) string {
	value := os.Getenv(varname)

	if value == "" {
		return defaultValue
	}

	return value
}

func outputFilename(outputDir string, binlogFilename string) string {
	basename := path.Base(binlogFilename)
	return path.Join(outputDir, fmt.Sprintf("%s.json", basename))
}
