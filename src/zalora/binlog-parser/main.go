package main

import (
	"flag"
	"fmt"
	"github.com/golang/glog"
	"os"
	"path"
	"strings"
	"zalora/binlog-parser/parser"
)

// global flags
var outputDirFlag = flag.String("output-dir", "/tmp", "Directory to dump output files to (default is system tmp dir)")
var prettyPrintJsonFlag = flag.Bool("pretty-json", false, "Pretty print json")
var includeTablesFlag = flag.String("include-tables", "", "Comma-separated list of tables to include")
var includeSchemasFlag = flag.String("include-schemas", "", "Comma-separated list of schemas to include")

// for parsing index only
var parsedIndexFilenameFlag = flag.String("parsed-index", "/tmp/parsed.index", "filename to keep state (will be created if it doesn't exist)")

func main() {
	flag.Usage = func() {
		printUsage()
	}

	flag.Parse()

	if len(os.Args) <= 1 {
		printUsage()

		return
	}

	switch os.Args[1] {
	case "file":
		if len(os.Args) == 2 {
			fmt.Fprint(os.Stderr, "Please provide a file to parse\n")
			os.Exit(1)
		}

		binlogFilename := os.Args[2]

		glog.V(1).Infof("Will parse file %s", binlogFilename)

		parseFunc := createBinlogParseFunc(createConsumerChain())
		err := parseFunc(binlogFilename)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Got error: %s\n", err)
			os.Exit(1)
		}

		break

	case "index":
		if len(os.Args) == 2 {
			fmt.Fprint(os.Stderr, "Please provide an index file to parse\n")
			os.Exit(1)
		}

		indexFilename := os.Args[2]

		glog.V(1).Infof("Will parse index file %s", indexFilename)

		err := parseFromBinlogIndex(
			indexFilename,
			*parsedIndexFilenameFlag,
			createBinlogParseFunc(createConsumerChain()),
		)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Got error: %s", err)
			os.Exit(1)
		}

		break

	default:
		printUsage()

		break
	}
}

func printUsage() {
	binName := path.Base(os.Args[0])

	usage := "Usage\t%s file [binlog file] [options ...]\n" +
		"\t%s index [index file] [options ...]\n\n" +
		"Options\n\n"

	fmt.Fprintf(os.Stderr, usage, binName, binName)

	flag.PrintDefaults()
}

func createConsumerChain() parser.ConsumerChain {
	chain := parser.NewConsumerChain()

	chain.OutputParsedFilesToDir(*outputDirFlag)
	glog.V(1).Infof("Output dir is %s", *outputDirFlag)

	chain.PrettyPrint(*prettyPrintJsonFlag)
	glog.V(1).Infof("Pretty print JSON %s", *prettyPrintJsonFlag)

	if includeTablesFlag != nil {
		includeTables := commaSeparatedListToArray(*includeTablesFlag)

		chain.IncludeTables(includeTables...)
		glog.V(1).Infof("Including tables %v", includeTables)
	}

	if includeSchemasFlag != nil {
		includeSchemas := commaSeparatedListToArray(*includeSchemasFlag)

		chain.IncludeTables(includeSchemas...)
		glog.V(1).Infof("Including schemas %v", includeSchemas)
	}

	return chain
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
