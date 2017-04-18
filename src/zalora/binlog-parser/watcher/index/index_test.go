package index

import (
	"testing"
	"fmt"
	"io/ioutil"
	"os"
	"path"
 	"zalora/binlog-parser/test"
)

func TestFileNotFound(t *testing.T) {
	_, err := NewIndex("/not/there")

	if err == nil {
		t.Fatal("Expected error when trying to create index from non-existing file")
	}
}

func TestIndex(t *testing.T) {
	t.Run("Append and Sync", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "test")
		defer os.Remove(tmpfile.Name())

		index, err := NewIndex(path.Join(test.GetDataDir(), "fixtures/mysql-index-file.01"))

		if err != nil {
			t.Fatal("Got error when creating index")
		}

		index.Append("/tmp/mysql-bin.88888")
		index.Append("/tmp/mysql-bin.99999")
		index.SyncFile(tmpfile.Name())

		assertFileContent(t, tmpfile.Name(), "/tmp/mysql-bin.000001\n/tmp/mysql-bin.88888\n/tmp/mysql-bin.99999\n")
	})

	t.Run("Diff same files", func(t *testing.T) {
		indexOne, err := NewIndex(path.Join(test.GetDataDir(), "fixtures/mysql-index-file.01"))

		if err != nil {
			t.Fatal("Got error when creating index")
		}

		indexTwo, err := NewIndex(path.Join(test.GetDataDir(), "fixtures/mysql-index-file.01"))

		if err != nil {
			t.Fatal("Got error when creating index")
		}

		diffedLines := indexOne.Diff(indexTwo)

		if len(diffedLines) != 0 {
			t.Fatal("Expected no difference between identical files")
		}
	})

	t.Run("Diff", func (t *testing.T) {
		indexMaster, err := NewIndex(path.Join(test.GetDataDir(), "fixtures/mysql-index-file.02"))

		if err != nil {
			t.Fatal("Got error when creating index")
		}

		indexToBeDiffed, err := NewIndex(path.Join(test.GetDataDir(), "fixtures/mysql-index-file.01"))

		if err != nil {
			t.Fatal("Got error when creating index")
		}

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
