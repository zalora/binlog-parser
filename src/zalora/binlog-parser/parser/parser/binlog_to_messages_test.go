package parser

import (
	"testing"
	"errors"
	"fmt"
	"path"
	"io/ioutil"
	"strings"
	"encoding/json"
 	"zalora/binlog-parser/database"
 	"zalora/binlog-parser/parser/messages"
 	"zalora/binlog-parser/test"
)

func TestBinlogToMessages(t *testing.T) {
	db := database.GetDatabaseInstance(test.TEST_DB_CONNECTION_STRING)
	defer db.Close()

	t.Run("Binlog file not found", func(t *testing.T) {
		tableMap := database.NewTableMap(db)

		_, err := doParseBinlogToMessages("not_there", tableMap)

		if err == nil {
			t.Fatal("Expected error when opening non-existant file")
		}
	})

	// 2 x insert, 5 x insert, 2 x update, 1 x delete
	t.Run("Parse binlog #1", func(t *testing.T) {
		tableMap := database.NewTableMap(db)

		collectedMessages, err := doParseBinlogToMessages("fixtures/mysql-bin.01", tableMap)

		if err != nil {
			t.Fatal("Expected to parse binlog")
		}

		assertMessages(t, collectedMessages, "fixtures/01.json")
	})

	// insert into a table that is dropped later on, fields not found
	t.Run("Parse binlog #2", func(t *testing.T) {
		tableMap := database.NewTableMap(db)

		collectedMessages, err := doParseBinlogToMessages("fixtures/mysql-bin.02", tableMap)

		if err != nil {
			t.Fatal("Expected to parse binlog")
		}

		assertMessages(t, collectedMessages, "fixtures/02.json")
	})

	t.Run("Consume callback returns error", func(t *testing.T) {
		tableMap := database.NewTableMap(db)

		consumeMessage := func (m messages.Message) error {
			return errors.New("Something went wrong")
		}

		err := ParseBinlogToMessages(prefixDataDir("fixtures/mysql-bin.01"), tableMap, consumeMessage)

		if err == nil {
			t.Fatal("Expected to get error from consumeMessage")
		}
	})
}

func doParseBinlogToMessages(binlogFileName string, tableMap database.TableMap) ([]messages.Message, error) {
	var collectedMessages []messages.Message

	consumeMessage := func (m messages.Message) error {
		collectedMessages = append(collectedMessages, m)
		return nil
	}

	err := ParseBinlogToMessages(prefixDataDir(binlogFileName), tableMap, consumeMessage)

	if err != nil {
		return nil, err
	}

	return collectedMessages, nil
}

func assertMessages(t *testing.T, messages []messages.Message, compareAgainstFileName string) {
	fileContent, err := ioutil.ReadFile(prefixDataDir(compareAgainstFileName))

	if err != nil {
		t.Fatal(fmt.Sprintf("Failed to compare against file - failed to get file content from %s", compareAgainstFileName))
	}

	messagesAsJson, err := json.MarshalIndent(messages, "", "    ")

	if err != nil {
		t.Fatal("Failed to compare against file - could not convert messages to json")
	}

	fileContentTrimmed := strings.Trim(string(fileContent), "\n")

	if string(messagesAsJson) != fileContentTrimmed {
		t.Fatal(fmt.Sprintf("json does not match when comparing against %s - got\n%s", compareAgainstFileName, messagesAsJson))
	}
}

func prefixDataDir(filePath string) string {
	return path.Join(test.GetDataDir(), filePath)
}
