package parser

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
	"zalora/binlog-parser/parser/messages"
)

func TestConsumerChain(t *testing.T) {
	messageMinimal := messages.NewQueryMessage(
		messages.NewMinimalMessageHeader(time.Now(), 100),
		messages.SqlQuery("SELECT * FROM table"),
	)

	messageOne := messages.NewQueryMessage(
		messages.NewMessageHeader("database_name", "table_name", time.Now(), 100, 100),
		messages.SqlQuery("SELECT * FROM table"),
	)

	t.Run("No predicates, no collectors", func(t *testing.T) {
		chain := NewConsumerChain()
		err := chain.consumeMessage(messageMinimal)

		if err != nil {
			t.Fatal("Failed to consume message")
		}
	})

	t.Run("Collect as JSON file", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())

		chain := NewConsumerChain()
		chain.CollectAsJsonInFile(tmpfile)

		err := chain.consumeMessage(messageMinimal)

		if err != nil {
			t.Fatal("Failed to consume message")
		}

		assertJsonOutputNotEmpty(t, tmpfile)
	})

	t.Run("Filter schema, passes through", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())

		chain := NewConsumerChain()
		chain.IncludeSchemas("some_db", "database_name")
		chain.CollectAsJsonInFile(tmpfile)

		err := chain.consumeMessage(messageOne)

		if err != nil {
			t.Fatal("Failed to consume message")
		}

		assertJsonOutputNotEmpty(t, tmpfile)
	})

	t.Run("Filter schema, filtered out", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())

		chain := NewConsumerChain()
		chain.IncludeSchemas("some_db")
		chain.CollectAsJsonInFile(tmpfile)

		err := chain.consumeMessage(messageOne)

		if err != nil {
			t.Fatal("Failed to consume message")
		}

		assertJsonOutputEmpty(t, tmpfile)
	})

	t.Run("Filter table, passes through", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())

		chain := NewConsumerChain()
		chain.IncludeTables("some_table", "table_name")
		chain.CollectAsJsonInFile(tmpfile)

		err := chain.consumeMessage(messageOne)

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
		chain.CollectAsJsonInFile(tmpfile)

		err := chain.consumeMessage(messageOne)

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
