package parser

import (
	"testing"
	"fmt"
	"flag"
	"os"
	"path"
	"io/ioutil"
	"database/sql"
	"strings"
	"encoding/json"
 	"zalora/binlog-parser/parser/database"
 	"zalora/binlog-parser/parser/messages"
	_ "github.com/go-sql-driver/mysql"
)

func setupDb() *sql.DB {
	db, db_err := sql.Open("mysql", "root@/test_db")

	if db_err != nil {
		panic(db_err.Error())
	}

	db_err = db.Ping()

	if db_err != nil {
		panic(db_err.Error())
	}

	return db
}

func TestMain(m *testing.M) {
	flag.Set("alsologtostderr", "true")
	flag.Set("v", "5")

	os.Exit(m.Run())
}

func TestBinlogToMessages(t *testing.T) {
	db := setupDb()
	defer db.Close()

	t.Run("Binlog file not found", func(t *testing.T) {
		tableMap := database.NewTableMap(db)

		_, err := doParseBinlogToMessages("not_there", 0, tableMap)

		if err == nil {
			t.Fatal("Expected error when opening non-existant file")
		}
	})

	// 2 x insert, 5 x insert, 2 x update, 1 x delete
	t.Run("Parse binlog #1", func(t *testing.T) {
		tableMap := database.NewTableMap(db)

		collectedMessages, err := doParseBinlogToMessages("fixtures/mysql-bin.01", 0, tableMap)

		if err != nil {
			t.Fatal("Expected to parse binlog")
		}

		assertMessages(t, collectedMessages, "fixtures/01.json")
	})

	t.Run("Parse binlog #2", func(t *testing.T) {
		tableMap := database.NewTableMap(db)

		collectedMessages, err := doParseBinlogToMessages("fixtures/mysql-bin.02", 0, tableMap)

		if err != nil {
			t.Fatal("Expected to parse binlog")
		}

		assertMessages(t, collectedMessages, "fixtures/02.json")
	})

	// disabled for now, see https://github.com/siddontang/go-mysql/issues/127
	// t.Run("Parse binlog #1, starting from position", func(t *testing.T) {
	// 	tableMap := database.NewTableMap(db)

	// 	collectedMessages, err := doParseBinlogToMessages("fixtures/mysql-bin.01", 428, tableMap)

	// 	if err != nil {
	// 		t.Fatal("Expected to parse binlog")
	// 	}

	// 	assertMessages(t, collectedMessages, "fixtures/01.json")
	// })
}

func doParseBinlogToMessages(binlogFileName string, offset int64, tableMap database.TableMap) ([]messages.Message, error) {
	var collectedMessages []messages.Message

	consumeMessage := func (m messages.Message) {
		collectedMessages = append(collectedMessages, m)
	}

	err := ParseBinlogToMessages(path.Join(os.Getenv("DATA_DIR"), binlogFileName), offset, tableMap, consumeMessage)

	if err != nil {
		return nil, err
	}

	return collectedMessages, nil
}

func assertMessages(t *testing.T, messages []messages.Message, compareAgainstFileName string) {
	fileContent, err := ioutil.ReadFile(path.Join(os.Getenv("DATA_DIR"), compareAgainstFileName))

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
