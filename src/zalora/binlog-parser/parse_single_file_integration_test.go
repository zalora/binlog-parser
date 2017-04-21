// +build integration

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"zalora/binlog-parser/parser"
	"zalora/binlog-parser/test"
)

func TestParseBinlogFile(t *testing.T) {
	dataDir := os.Getenv("DATA_DIR")
	os.Setenv("DB_DSN", test.TEST_DB_CONNECTION_STRING)

	t.Run("binlog file not found", func(t *testing.T) {
		outputDir, _ := ioutil.TempDir("", "test")
		defer os.RemoveAll(outputDir)

		err := parseBinlogFile("/not/there", createConsumerChain(outputDir))

		if err == nil {
			t.Fatal("Expected error when parsing non-existing file")
		}
	})

	t.Run("output dir not found", func(t *testing.T) {
		binlogFilename := filepath.Join(dataDir, "fixtures/mysql-bin.01")
		err := parseBinlogFile(binlogFilename, createConsumerChain("/not/there"))

		if err == nil {
			t.Fatal("Expected error when parsing non-existing file")
		}
	})

	testCases := []struct {
		fixtureFilename     string
		expectedJsonFile    string
		expectedOutfilename string
	}{
		{"fixtures/mysql-bin.01", "fixtures/01.json", "mysql-bin.01.json"},
		{"fixtures/mysql-bin.02", "fixtures/02.json", "mysql-bin.02.json"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Parse binlog %s", tc.fixtureFilename), func(t *testing.T) {
			binlogFilename := filepath.Join(dataDir, tc.fixtureFilename)

			outputDir, _ := ioutil.TempDir("", "test")
			defer os.RemoveAll(outputDir)

			err := parseBinlogFile(binlogFilename, createConsumerChain(outputDir))

			if err != nil {
				t.Fatal(fmt.Sprintf("Expected no error when successfully parsing file %s", err))
			}

			outputFile := fmt.Sprintf("%s/%s", outputDir, tc.expectedOutfilename)

			assertFileExists(t, outputFile)
			assertFilesAreEqual(t, outputFile, filepath.Join(dataDir, tc.expectedJsonFile))
		})
	}
}

func assertFileExists(t *testing.T, filename string) {
	if _, err := os.Stat(filename); err != nil {
		t.Fatal(fmt.Sprintf("File %s does not exist", filename))
	}
}

func assertFilesAreEqual(t *testing.T, a string, b string) {
	f1, err1 := ioutil.ReadFile(a)

	if err1 != nil {
		t.Fatal(fmt.Sprintf("Failed to compare files: %s", err1))
	}

	f2, err2 := ioutil.ReadFile(b)

	if err2 != nil {
		t.Fatal(fmt.Sprintf("Failed to compare files: %s", err2))
	}

	f1Str := strings.TrimSpace(string(f1))
	f2Str := strings.TrimSpace(string(f2))

	if f1Str != f2Str {
		t.Fatal(fmt.Sprintf("Files do not match\nFile 1:\n==========\n%s\n==========\nFile 2\n%s\n==========", f1Str, f2Str))
	}
}

func createConsumerChain(outputDir string) parser.ConsumerChain {
	consumerChain := parser.NewConsumerChain()
	consumerChain.PrettyPrint(true)
	consumerChain.OutputParsedFilesToDir(outputDir)

	return consumerChain
}
