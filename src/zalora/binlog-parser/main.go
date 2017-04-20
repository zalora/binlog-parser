package main

import (
	"flag"
	"fmt"
	"os"
)

// global flags
var outputDirFlag = flag.String("output-dir", "/tmp", "Directory to dump output files to")
var prettyJsonFlag = flag.Bool("pretty-json", false, "Pretty print json")

// parse single file
var binlogFilenameFlag = flag.String("file", "", "binlog file to parse")

// parse multiple files, keeping state
var binlogIndexFilenameFlag = flag.String("binlog-index", "", "binlog file to watch")
var parsedIndexFilenameFlag = flag.String("parsed-index", "/tmp/parsed.index", "filename to keep state (will be created if it doesn't exist)")

func main() {
	flag.Parse()

	if binlogFilenameFlag != nil {
		parseFunc := createBinlogParseFunc(*prettyJsonFlag)
		err := parseFunc(*binlogFilenameFlag, *outputDirFlag)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Got error: %s", err)
			os.Exit(1)
		}

		return
	}

	if binlogIndexFilenameFlag != nil {
		err := parseFromBinlogIndex(
			*binlogIndexFilenameFlag,
			*parsedIndexFilenameFlag,
			*outputDirFlag,
			createBinlogParseFunc(*prettyJsonFlag),
		)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Got error: %s", err)
			os.Exit(1)
		}

		return
	}
}
