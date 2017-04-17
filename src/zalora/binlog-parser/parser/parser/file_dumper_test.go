package parser

import (
	"os"
	"io/ioutil"
	"testing"
	"time"
	"zalora/binlog-parser/parser/messages"
)

func TestDumpMessageAsJsonToFile(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "messages.json")

	if err != nil {
		t.Fatal("Failed to open tmpfile")
	}

	defer os.Remove(tmpfile.Name())

	messageConsumerFunc := DumpMessageAsJsonToFile(tmpfile)

	header := messages.NewMinimalMessageHeader(time.Now(), 100)
	message := messages.NewQueryMessage(header, messages.SqlQuery("SELECT * FROM table"))

	messageConsumerFunc(message)

	fileContent, err := ioutil.ReadFile(tmpfile.Name())

	if err != nil {
		t.Fatal("Failed to read tmp file")
	}

	if len(fileContent) == 0 {
		t.Fatal("Failed to dump JSON to file - tmp file is empty")
	}
}
