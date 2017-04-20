// +build unit

package main

import (
	"testing"
)

func TestOutputFilename(t *testing.T) {
	outputFilename := outputFilename("/some/dir", "some_file.bin")

	if outputFilename != "/some/dir/some_file.bin.json" {
		t.Fatal("Wrong output file name generated")
	}
}
