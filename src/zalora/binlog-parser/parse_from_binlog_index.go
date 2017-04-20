package main

import (
	"github.com/golang/glog"
	"os"
	"sync"
	"zalora/binlog-parser/index"
)

func parseFromBinlogIndex(
	binlogIndexFilename string,
	parsedIndexFilename string,
	outputDir string,
	parseFunc binlogParseFunc,
) error {
	glog.V(1).Info("Parsing binlog index")

	if _, err := os.Stat(binlogIndexFilename); os.IsNotExist(err) {
		return err
	}

	parsedIndexFile, err := os.OpenFile(parsedIndexFilename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)

	if err != nil {
		return err
	}

	defer parsedIndexFile.Close()

	binlogIndexFile, err := os.Open(binlogIndexFilename)

	if err != nil {
		return err
	}

	defer binlogIndexFile.Close()

	parsedIndex := index.NewIndex(parsedIndexFile)
	binlogIndex := index.NewIndex(binlogIndexFile)

	filesToParse := binlogIndex.Diff(parsedIndex)

	if len(filesToParse) <= 1 {
		glog.V(1).Infof("No binlogs to parse, found %v", filesToParse)
		return nil
	}

	glog.V(1).Infof("Will parse %v", filesToParse)

	parsedFiles, err := parseMultipleBinlogFiles(
		outputDir,
		filesToParse[:len(filesToParse)-1],
		parseFunc,
	)

	if err != nil {
		return err
	}

	glog.V(1).Infof("Successfully parsed %v, updating parsed index", parsedFiles)

	parsedIndex.Append(parsedFiles...)
	err = parsedIndex.Sync()

	if err != nil {
		return err
	}

	return nil
}

func parseMultipleBinlogFiles(outputDir string, filesToParse []string, parseFunc binlogParseFunc) ([]string, error) {
	parsedFiles := make([]string, len(filesToParse))
	errors := make([]error, len(filesToParse))

	var wg sync.WaitGroup
	wg.Add(len(filesToParse))

	for i, binlogFilename := range filesToParse {
		glog.V(1).Infof("Parsing %s", binlogFilename)

		go func(i int, f string) {
			defer wg.Done()

			err := parseFunc(f, outputDir)

			if err != nil {
				errors[i] = err
			} else {
				parsedFiles[i] = f
			}
		}(i, binlogFilename)
	}

	wg.Wait()

	firstError := findFirstError(errors)
	p := removeEmptyArrayEntries(parsedFiles)

	glog.V(1).Infof("Parsed %v files - first error (if any): %s", p, firstError)

	return p, firstError
}

func removeEmptyArrayEntries(strs []string) []string {
	var tmp []string

	for _, str := range strs {
		tmp = append(tmp, str)
	}

	return tmp
}

func findFirstError(errors []error) error {
	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}
