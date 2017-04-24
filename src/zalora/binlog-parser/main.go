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

var prettyPrintJsonFlag = flag.Bool("prettyprint", false, "Pretty print json")
var includeTablesFlag = flag.String("include_tables", "", "comma-separated list of tables to include")
var includeSchemasFlag = flag.String("include_schemas", "", "comma-separated list of schemas to include")

func main() {
	flag.Usage = func() {
		printUsage()
	}

	flag.Parse()

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	binlogFilename := os.Args[2]

	glog.V(1).Infof("Will parse file %s", binlogFilename)

	parseFunc := createBinlogParseFunc(consumerChainFromArgs())
	err := parseFunc(binlogFilename)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Got error: %s\n", err)
		os.Exit(1)
	}
}

func consumerChainFromArgs() parser.ConsumerChain {
	chain := parser.NewConsumerChain()

	chain.CollectAsJson(os.Stdout, *prettyPrintJsonFlag)
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

func printUsage() {
	binName := path.Base(os.Args[0])

	usage := "Parse a binlog file, dump JSON to stdout. Includes options to filter by schema and table.\n" +
		"Reads from information_schema database to find out the field names for a row event.\n\n" +
		"Usage:\t%s binlog [options ...]\n\n" +
		"Options are:\n\n"

	fmt.Fprintf(os.Stderr, usage, binName)

	flag.PrintDefaults()

	envVars := "\nRequired environment variables:\n\n" +
		"DB_DSN\t Database connection string, needs read access to information_schema\n"

	fmt.Fprint(os.Stderr, envVars)
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
