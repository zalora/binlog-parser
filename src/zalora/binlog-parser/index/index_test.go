// +build unit

package index

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"zalora/binlog-parser/test"
)

func TestIndex(t *testing.T) {
	t.Run("Append and Sync", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "test")
		defer os.Remove(tmpfile.Name())

		indexFile, _ := os.Open(path.Join(test.GetDataDir(), "fixtures/mysql-index-file.01"))
		index := NewIndex(indexFile)

		index.Append("/tmp/mysql-bin.88888")
		index.Append("/tmp/mysql-bin.99999")
		index.SyncFile(tmpfile.Name())

		assertFileContent(t, tmpfile.Name(), "/tmp/mysql-bin.000001\n/tmp/mysql-bin.88888\n/tmp/mysql-bin.99999\n")
	})

	t.Run("Diff same files", func(t *testing.T) {
		indexOneFile, _ := os.Open(path.Join(test.GetDataDir(), "fixtures/mysql-index-file.01"))
		indexOne := NewIndex(indexOneFile)

		indexTwoFile, _ := os.Open(path.Join(test.GetDataDir(), "fixtures/mysql-index-file.01"))
		indexTwo := NewIndex(indexTwoFile)

		diffedLines := indexOne.Diff(indexTwo)

		if len(diffedLines) != 0 {
			t.Fatal("Expected no difference between identical files")
		}
	})

	t.Run("Diff", func(t *testing.T) {
		indexMasterFile, _ := os.Open(path.Join(test.GetDataDir(), "fixtures/mysql-index-file.02"))
		indexMaster := NewIndex(indexMasterFile)

		indexToBeDiffedFile, _ := os.Open(path.Join(test.GetDataDir(), "fixtures/mysql-index-file.01"))
		indexToBeDiffed := NewIndex(indexToBeDiffedFile)

		diffedLines := indexMaster.Diff(indexToBeDiffed)

		if len(diffedLines) != 1 {
			t.Fatal("Expected diff to be found")
		}

		if diffedLines[0] != "/tmp/mysql-bin.000002" {
			t.Fatal(fmt.Sprintf("Unexpected diff line, got %s", diffedLines[0]))
		}
	})
}

func assertFileContent(t *testing.T, filename string, expectedFileContent string) {
	fileContent, err := ioutil.ReadFile(filename)

	if err != nil {
		t.Fatal("Failed to read file")
	}

	if string(fileContent) != expectedFileContent {
		t.Fatal(fmt.Sprintf("Failed to assert file content, got:\n%s", fileContent))
	}
}
