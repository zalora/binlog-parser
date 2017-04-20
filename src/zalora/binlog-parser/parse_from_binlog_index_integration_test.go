// +build integration

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"zalora/binlog-parser/test"
)

func TestParseFromBinlogIndex(t *testing.T) {
	dataDir := os.Getenv("DATA_DIR")
	os.Setenv("DB_DSN", test.TEST_DB_CONNECTION_STRING)

	binlogFilenameOne := filepath.Join(dataDir, "fixtures/mysql-bin.01")
	binlogFilenameTwo := filepath.Join(dataDir, "fixtures/mysql-bin.02")
	binlogFilenameThree := filepath.Join(dataDir, "fixtures/mysql-bin.empty")

	t.Run("binlog index file not found", func(t *testing.T) {
		parsedIndexFile, err := ioutil.TempFile("", "test")
		defer os.Remove(parsedIndexFile.Name())

		outputDir, _ := ioutil.TempDir("", "test")
		defer os.RemoveAll(outputDir)

		parseFunc := createBinlogParseFunc(createConsumerChain(outputDir))
		err = parseFromBinlogIndex("/not/there", parsedIndexFile.Name(), parseFunc)

		if err == nil {
			t.Fatal("Expected error when parsing non-existing file")
		}
	})

	t.Run("parsed index file in non-existing directory", func(t *testing.T) {
		binlogIndexFile, err := ioutil.TempFile("", "test")
		defer os.Remove(binlogIndexFile.Name())

		outputDir, _ := ioutil.TempDir("", "test")
		defer os.RemoveAll(outputDir)

		parseFunc := createBinlogParseFunc(createConsumerChain(outputDir))
		err = parseFromBinlogIndex(binlogIndexFile.Name(), "/not/there", parseFunc)

		if err == nil {
			t.Fatal("Expected error when parsing non-existing file")
		}
	})

	t.Run("Parse from binlog index and update parsed index", func(t *testing.T) {
		binlogIndexFile, err := ioutil.TempFile("", "test")
		defer os.Remove(binlogIndexFile.Name())

		parsedIndexFile, err := ioutil.TempFile("", "test")
		defer os.Remove(parsedIndexFile.Name())

		outputDir, _ := ioutil.TempDir("", "test")
		defer os.RemoveAll(outputDir)

		binlogIndexFile.WriteString(fmt.Sprintf("%s\n", binlogFilenameOne))
		binlogIndexFile.WriteString(fmt.Sprintf("%s\n", binlogFilenameTwo))
		binlogIndexFile.WriteString(fmt.Sprintf("%s\n", binlogFilenameThree))

		parseFunc := createBinlogParseFunc(createConsumerChain(outputDir))
		err = parseFromBinlogIndex(binlogIndexFile.Name(), parsedIndexFile.Name(), parseFunc)

		if err != nil {
			t.Fatal("Expected no error when parsing file")
		}

		parsedIndexFileContent, _ := ioutil.ReadFile(parsedIndexFile.Name())
		parsedFiles := strings.Split(strings.TrimSpace(string(parsedIndexFileContent)), "\n")

		if len(parsedFiles) != 2 {
			t.Fatal(fmt.Sprintf("Wrong files parsed parsed: %v", parsedFiles))
		}

		if !strings.HasSuffix(parsedFiles[0], "mysql-bin.01") {
			t.Fatal("Wrong file parsed")
		}

		if !strings.HasSuffix(parsedFiles[1], "mysql-bin.02") {
			t.Fatal("Wrong file parsed")
		}
	})

	t.Run("Only one file in binlog index, ignored", func(t *testing.T) {
		binlogIndexFile, err := ioutil.TempFile("", "test")
		defer os.Remove(binlogIndexFile.Name())

		parsedIndexFile, err := ioutil.TempFile("", "test")
		defer os.Remove(parsedIndexFile.Name())

		outputDir, _ := ioutil.TempDir("", "test")
		defer os.RemoveAll(outputDir)

		binlogIndexFile.WriteString(fmt.Sprintf("%s\n", binlogFilenameOne))

		parseFunc := createBinlogParseFunc(createConsumerChain(outputDir))
		err = parseFromBinlogIndex(binlogIndexFile.Name(), parsedIndexFile.Name(), parseFunc)

		if err != nil {
			t.Fatal("Expected no error when parsing file")
		}

		parsedIndexFileContent, _ := ioutil.ReadFile(parsedIndexFile.Name())

		if strings.TrimSpace(string(parsedIndexFileContent)) != "" {
			t.Fatal(fmt.Sprintf("Expected no files to be parsed %s", parsedIndexFileContent))
		}
	})

	t.Run("Parse from binlog index, one file already parsed", func(t *testing.T) {
		binlogIndexFile, err := ioutil.TempFile("", "test")
		defer os.Remove(binlogIndexFile.Name())

		parsedIndexFile, err := ioutil.TempFile("", "test")
		defer os.Remove(parsedIndexFile.Name())

		outputDir, _ := ioutil.TempDir("", "test")
		defer os.RemoveAll(outputDir)

		binlogIndexFile.WriteString(fmt.Sprintf("%s\n", binlogFilenameOne))
		binlogIndexFile.WriteString(fmt.Sprintf("%s\n", binlogFilenameTwo))
		binlogIndexFile.WriteString(fmt.Sprintf("%s\n", binlogFilenameThree))

		parsedIndexFile.WriteString(fmt.Sprintf("%s\n", binlogFilenameOne))

		parseFunc := createBinlogParseFunc(createConsumerChain(outputDir))
		err = parseFromBinlogIndex(binlogIndexFile.Name(), parsedIndexFile.Name(), parseFunc)

		if err != nil {
			t.Fatal("Expected no error when parsing file")
		}

		parsedIndexFileContent, _ := ioutil.ReadFile(parsedIndexFile.Name())
		parsedFiles := strings.Split(strings.TrimSpace(string(parsedIndexFileContent)), "\n")

		if len(parsedFiles) != 2 {
			t.Fatal(fmt.Sprintf("Wrong files parsed parsed: %v", parsedFiles))
		}

		if !strings.HasSuffix(parsedFiles[0], "mysql-bin.01") {
			t.Fatal("Wrong file parsed")
		}

		if !strings.HasSuffix(parsedFiles[1], "mysql-bin.02") {
			t.Fatal("Wrong file parsed")
		}
	})

	t.Run("output dir not found", func(t *testing.T) {
		binlogIndexFile, err := ioutil.TempFile("", "test")
		defer os.Remove(binlogIndexFile.Name())

		parsedIndexFile, err := ioutil.TempFile("", "test")
		defer os.Remove(parsedIndexFile.Name())

		outputDir, _ := ioutil.TempDir("", "test")
		defer os.RemoveAll(outputDir)

		binlogIndexFile.WriteString(fmt.Sprintf("%s\n", binlogFilenameOne))
		binlogIndexFile.WriteString(fmt.Sprintf("%s\n", binlogFilenameTwo))

		parseFunc := createBinlogParseFunc(createConsumerChain("/not/there"))
		err = parseFromBinlogIndex(binlogIndexFile.Name(), parsedIndexFile.Name(), parseFunc)

		if err == nil {
			t.Fatal("Expected error when output dir doesn't exist")
		}
	})
}
