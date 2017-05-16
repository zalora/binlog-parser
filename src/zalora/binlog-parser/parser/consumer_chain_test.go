// +build unit

package parser

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
	"zalora/binlog-parser/parser/messages"
)

func TestConsumerChain(t *testing.T) {
	messageOne := messages.NewQueryMessage(
		messages.NewMessageHeader("database_name", "table_name", time.Now(), 100, 100),
		messages.SqlQuery("SELECT * FROM table"),
	)

	messageTwo := messages.NewQueryMessage(
		messages.NewMessageHeader("database_name", "table_name", time.Now(), 100, 100),
		messages.SqlQuery("SELECT * FROM table"),
	)

	t.Run("No predicates, no collectors", func(t *testing.T) {
		chain := NewConsumerChain()
		err := chain.consumeMessage(messageOne)

		if err != nil {
			t.Fatal("Failed to consume message")
		}
	})

	t.Run("Collect as JSON file", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())

		chain := NewConsumerChain()
		chain.CollectAsJson(tmpfile, true)

		err := chain.consumeMessage(messageOne)

		if err != nil {
			t.Fatal("Failed to consume message")
		}

		assertJsonOutputNotEmpty(t, tmpfile)
	})

	t.Run("Filter schema, passes through", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())

		chain := NewConsumerChain()
		chain.CollectAsJson(tmpfile, true)
		chain.IncludeSchemas("some_db", "database_name")

		err := chain.consumeMessage(messageTwo)

		if err != nil {
			t.Fatal("Failed to consume message")
		}

		assertJsonOutputNotEmpty(t, tmpfile)
	})

	t.Run("Filter schema, filtered out", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())

		chain := NewConsumerChain()
		chain.CollectAsJson(tmpfile, true)
		chain.IncludeSchemas("some_db")

		err := chain.consumeMessage(messageTwo)

		if err != nil {
			t.Fatal("Failed to consume message")
		}

		assertJsonOutputEmpty(t, tmpfile)
	})

	t.Run("Filter table, passes through", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())

		chain := NewConsumerChain()
		chain.CollectAsJson(tmpfile, true)
		chain.IncludeTables("some_table", "table_name")

		err := chain.consumeMessage(messageTwo)

		if err != nil {
			t.Fatal("Failed to consume message")
		}

		assertJsonOutputNotEmpty(t, tmpfile)
	})

	t.Run("Filter table, filtered out", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())

		chain := NewConsumerChain()
		chain.IncludeTables("some_table")
		chain.CollectAsJson(tmpfile, true)

		err := chain.consumeMessage(messageTwo)

		if err != nil {
			t.Fatal("Failed to consume message")
		}

		assertJsonOutputEmpty(t, tmpfile)
	})
}

func assertJsonOutputNotEmpty(t *testing.T, tmpfile *os.File) {
	fileContent, err := ioutil.ReadFile(tmpfile.Name())

	if err != nil {
		t.Fatal("Failed to read tmp file")
	}

	if len(fileContent) == 0 {
		t.Fatal("Failed to dump JSON to file - tmp file is empty")
	}
}

func assertJsonOutputEmpty(t *testing.T, tmpfile *os.File) {
	fileContent, err := ioutil.ReadFile(tmpfile.Name())

	if err != nil {
		t.Fatal("Failed to read tmp file")
	}

	if len(fileContent) != 0 {
		t.Fatal("Expected JSON file to be empty")
	}
}
