// +build integration

package main

import (
	"bytes"
	"fmt"
	"io"
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

	createConsumerChain := func(stream io.Writer) parser.ConsumerChain {
		chain := parser.NewConsumerChain()
		chain.CollectAsJson(stream, true)

		return chain
	}

	t.Run("binlog file not found", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "test")
		defer os.RemoveAll(tmpfile.Name())

		err := parseBinlogFile("/not/there", test.TEST_DB_CONNECTION_STRING, createConsumerChain(tmpfile))

		if err == nil {
			t.Fatal("Expected error when parsing non-existing file")
		}
	})

	testCases := []struct {
		fixtureFilename  string
		expectedJsonFile string
		includeTables    []string
		includeSchemas   []string
	}{
		{"fixtures/mysql-bin.01", "fixtures/01.json", nil, nil},                                  // inserts and updates
		{"fixtures/mysql-bin.02", "fixtures/02.json", nil, nil},                                  // create table, insert
		{"fixtures/mysql-bin.03", "fixtures/03.json", nil, nil},                                  // insert 2 rows, update 2 rows, update 3 rows
		{"fixtures/mysql-bin.04", "fixtures/04.json", nil, nil},                                  // large insert (1000)
		{"fixtures/mysql-bin.05", "fixtures/05.json", nil, nil},                                  // DROP TABLE ... queries only
		{"fixtures/mysql-bin.06", "fixtures/06.json", nil, nil},                                  // table schema doesn't match anymore
		{"fixtures/mysql-bin.07", "fixtures/07.json", nil, nil},                                  // mariadb format, create table, insert two rows
		{"fixtures/mysql-bin.01", "fixtures/01-include-table.json", []string{"buildings"}, nil},  // include tables
		{"fixtures/mysql-bin.01", "fixtures/01-no-events.json", []string{"unknown_table"}, nil},  // only unknown table is included - no events parsed
		{"fixtures/mysql-bin.01", "fixtures/01.json", nil, []string{"test_db"}},                  // inlcude schemas
		{"fixtures/mysql-bin.01", "fixtures/01-no-events.json", nil, []string{"unknown_schema"}}, // only unknown schema is included - no events parsed
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Parse binlog %s", tc.fixtureFilename), func(t *testing.T) {
			var buffer bytes.Buffer
			binlogFilename := filepath.Join(dataDir, tc.fixtureFilename)

			chain := createConsumerChain(&buffer)

			if tc.includeTables != nil {
				chain.IncludeTables(tc.includeTables...)
			}

			if tc.includeSchemas != nil {
				chain.IncludeSchemas(tc.includeSchemas...)
			}

			err := parseBinlogFile(binlogFilename, test.TEST_DB_CONNECTION_STRING, chain)

			if err != nil {
				t.Fatal(fmt.Sprintf("Expected no error when successfully parsing file %s", err))
			}

			assertJson(t, buffer, filepath.Join(dataDir, tc.expectedJsonFile))
		})
	}
}

func assertJson(t *testing.T, buffer bytes.Buffer, expectedJsonFile string) {
	expectedJson, err := ioutil.ReadFile(expectedJsonFile)

	if err != nil {
		t.Fatal(fmt.Sprintf("Failed to open expected JSON file: %s", err))
	}

	expected := strings.TrimSpace(string(expectedJson))
	actual := strings.TrimSpace(buffer.String())

	if expected != actual {
		errorMessage := fmt.Sprintf(
			"JSON file %s does not match\nExpected:\n==========\n%s\n==========\nActual generated:\n%s\n==========",
			expectedJsonFile,
			expected,
			actual,
		)

		t.Fatal(errorMessage)
	}
}
