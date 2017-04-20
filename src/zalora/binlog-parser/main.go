package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"zalora/binlog-parser/parser"
)

// global flags
var outputDirFlag = flag.String("output-dir", "/tmp", "Directory to dump output files to")
var prettyJsonFlag = flag.Bool("pretty-json", false, "Pretty print json")

var includeTablesFlag = flag.String("include-tables", "", "Comma-separated list of tables to include")
var includeSchemasFlag = flag.String("include-schemas", "", "Comma-separated list of schemas to include")

// parse single file
var binlogFilenameFlag = flag.String("file", "", "binlog file to parse")

// parse multiple files, keeping state
var binlogIndexFilenameFlag = flag.String("binlog-index", "", "binlog file to watch")
var parsedIndexFilenameFlag = flag.String("parsed-index", "/tmp/parsed.index", "filename to keep state (will be created if it doesn't exist)")

func main() {
	flag.Parse()

	chain := parser.NewConsumerChain()
	chain.OutputParsedFilesToDir(*outputDirFlag)
	chain.PrettyPrint(*prettyJsonFlag)

	if *includeTablesFlag != "" {
		chain.IncludeTables(commaSeparatedListToArray(*includeTablesFlag)...)
	}

	if *includeSchemasFlag != "" {
		chain.IncludeTables(commaSeparatedListToArray(*includeSchemasFlag)...)
	}

	if binlogFilenameFlag != nil {
		parseFunc := createBinlogParseFunc(chain)
		err := parseFunc(*binlogFilenameFlag)

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
			createBinlogParseFunc(chain),
		)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Got error: %s", err)
			os.Exit(1)
		}

		return
	}
}

func commaSeparatedListToArray(str string) []string {
	var arr []string

	for _, item := range strings.Split(str, ",") {
		item = strings.TrimSpace(item)

		if item != "" {
			arr = append(arr, item)
		}
	}

	return arr
}
