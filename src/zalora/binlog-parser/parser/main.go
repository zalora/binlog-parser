package main

import (
	"encoding/json"
	"os"
	"fmt"
	"strings"
	"database/sql"
	"zalora/binlog-parser/parser/messages"
	"zalora/binlog-parser/parser/conversion"
	"zalora/binlog-parser/parser/database"
	"github.com/siddontang/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
)

func DumpMessage(messages ...messages.Message) {
	for _,message := range messages {
		b, err := json.MarshalIndent(message, "", "    ")

		if err != nil {
			fmt.Fprintf(os.Stdout, "JSON ERROR %s\n", err)
		} else {
			fmt.Fprintf(os.Stdout, "%s\n", b)
		}
	}
}

func main() {
	name := os.Args[1]

	// db connection
	db, db_err := sql.Open("mysql", "root@/test_db")

	if db_err != nil {
		panic(db_err.Error()) // @FIXME proper error handling
	}

	defer db.Close()

	db_err = db.Ping()

	if db_err != nil {
		panic(db_err.Error()) // @FIXME proper error handling
	}

	rowRowsEventBuffer := NewRowsEventBuffer()
	tableMap := database.NewTableMap(db)

	// parse bin logs
	p := replication.NewBinlogParser()

	f := func(e *replication.BinlogEvent) error {
		//e.Dump(os.Stdout)
		switch e.Header.EventType {
		case replication.QUERY_EVENT:
			queryEvent := e.Event.(*replication.QueryEvent)
			query := string(queryEvent.Query)

			fmt.Fprintf(os.Stdout, ">>> query event %s\n", query)

			if strings.ToUpper(strings.Trim(query, " ")) == "BEGIN" {
				fmt.Fprintf(os.Stdout, "> beginning transaction\n")
			} else {
				DumpMessage(conversion.ConvertQueryEventToMessage(*e.Header, *queryEvent))
			}

			break

		case replication.XID_EVENT:
			xidEvent := e.Event.(*replication.XIDEvent)
			xId := uint64(xidEvent.XID)

			fmt.Fprintf(os.Stdout, ">>> ending transaction XID %d\n", xId)
			DumpMessage(conversion.ConvertRowsEventsToMessages(xId, rowRowsEventBuffer.Drain()) ...)

			break

		case replication.TABLE_MAP_EVENT:
			tableMapEvent := e.Event.(*replication.TableMapEvent)

			schema := string(tableMapEvent.Schema)
			table := string(tableMapEvent.Table)
			tableId := uint64(tableMapEvent.TableID)

			tableMap.Add(tableId, schema, table)

			break

		case replication.WRITE_ROWS_EVENTv2:
			fallthrough
		case replication.UPDATE_ROWS_EVENTv2:
			fallthrough
 		case replication.DELETE_ROWS_EVENTv2:
			rowsEvent := e.Event.(*replication.RowsEvent)

			tableId := uint64(rowsEvent.TableID)
			tableMetadata, ok := tableMap.LookupTableMetadata(tableId)

			if ok == false {
				// @TODO handle this
				fmt.Fprintf(os.Stdout, "@@@ ERROR table info NOT FOUND\n")
				break
			}

			rowRowsEventBuffer.BufferRowsEventData(
				conversion.RowsEventData{
					BinlogEventHeader: *e.Header,
					BinlogEvent: *rowsEvent,
					TableMetadata: tableMetadata,
				},
			)

			break

		default:
			break
		}

		return nil
	}

	err := p.ParseFile(name, 0, f)

	if err != nil {
		println(err)
	}
}
