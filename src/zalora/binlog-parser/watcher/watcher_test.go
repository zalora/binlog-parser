package watcher

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestParseFiles(t *testing.T) {
	t.Run("Only one file, not parsed", func(t *testing.T) {
		filesToParse := []string{"/file/one"}
		parseFunc := func(file string) error { return nil }

		parsedFiles, _ := parseFiles(filesToParse, parseFunc)

		if len(parsedFiles) != 0 {
			t.Fatal("Expected no files to be parsed")
		}
	})

	t.Run("Multiple files parsed, last one ignored", func(t *testing.T) {
		expectedParsedFiles := []string{"/file/one", "/file/two"}

		filesToParse := []string{"/file/one", "/file/two", "/file/three"}
		parseFunc := func(file string) error { return nil }

		parsedFiles, _ := parseFiles(filesToParse, parseFunc)

		if !reflect.DeepEqual(parsedFiles, expectedParsedFiles) {
			t.Fatal("Wrong files parsed")
		}
	})

	t.Run("Multiple files to be parsed, file returns error", func(t *testing.T) {
		filesToParse := []string{"/file/one", "/file/two", "/file/three"}
		parseFunc := func(file string) error {
			if strings.HasSuffix(file, "/two") {
				return errors.New("Some error with file two")
			}

			return nil
		}

		parsedFiles, err := parseFiles(filesToParse, parseFunc)

		if len(parsedFiles) != 0 {
			t.Fatal("Expected no files to be parsed")
		}

		if err == nil {
			t.Fatal("Expected error to be returned")
		}
	})
}
