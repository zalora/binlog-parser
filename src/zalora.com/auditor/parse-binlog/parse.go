package main

import (
	"encoding/json"
	"time"
	"os"
	"fmt"
	"strings"
	"database/sql"
	"github.com/siddontang/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
)

type TableMetadata struct {
	schema string
	table string
	fields map[int]string
}

func GetFields(db* sql.DB, schema string, table string) map[int]string {
	rows, db_err := db.Query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", schema, table)

	if db_err != nil {
		panic(db_err.Error()) // @FIXME proper error handling
	}

	defer rows.Close()

	fields := make(map[int]string)
	i := 0

	var columnName string
	for rows.Next() {
		db_err := rows.Scan(&columnName)

		if db_err != nil {
			panic(db_err.Error()) // @FIXME proper error handling
		}

		fields[i] = columnName
		i++;
	}

	return fields
}

func RowData(rowsEvent *replication.RowsEvent, columnNames map[int]string) []map[string]interface{} {
	ret := make([]map[string]interface{}, 0)

	for _, rows := range rowsEvent.Rows {
		data := make(map[string]interface{})
		unknownCount := 0;

		for j, d := range rows {
			columnName, exists := columnNames[j]

			if exists == false {
				columnName = fmt.Sprintf("(unknown_%d)", unknownCount)
				unknownCount++
			}

			data[columnName] = d
		}

		ret = append(ret, data)
	}

	return ret
}

type MessageHeader struct {
	Schema string
	Table string
	BinlogMessageTime string
	MessageTime string
	BinlogPosition uint32
	XId uint64
}

func NewMessageHeader(schema string, table string, binlogMessageTime time.Time, binlogPosition uint32, xId uint64) MessageHeader {
	return MessageHeader {
		Schema: schema,
		Table: table,
		BinlogMessageTime: binlogMessageTime.Format(time.RFC3339),
		MessageTime: time.Now().Format(time.RFC3339Nano),
		BinlogPosition: binlogPosition,
		XId: xId,
	}
}

type MessageHeaderBuilder func(uint64) MessageHeader

func NewMessageHeaderBuilder(schema string, table string, binlogMessageTime time.Time, binlogPosition uint32) MessageHeaderBuilder {
	return func (xId uint64) MessageHeader {
		return NewMessageHeader(schema, table, binlogMessageTime, binlogPosition, xId)
	}
}

type Message interface {
}

type MessageBuilder func(uint64) Message

type UpdateMessage struct {
	Header MessageHeader
	Type string
	OldData map[string]interface{}
	NewData map[string]interface{}
}

func NewUpdateMessage(header MessageHeader, oldData map[string]interface{}, newData map[string]interface{}) UpdateMessage {
	return UpdateMessage{Header: header, Type: "UPDATE", OldData: oldData, NewData: newData}
}

type InsertMessage struct {
	Header MessageHeader
	Type string
	Data map[string]interface{}
}

func NewInsertMessage(header MessageHeader, data map[string]interface{}) InsertMessage {
	return InsertMessage{Header: header, Type: "INSERT", Data: data}
}


type DeleteMessage struct {
	Header MessageHeader
	Type string
	Data map[string]interface{}
}

func NewDeleteMessage(header MessageHeader, data map[string]interface{}) DeleteMessage {
	return DeleteMessage{Header: header, Type: "DELETE", Data: data}
}






func CreateUpdateMessagesFromRowData(header MessageHeader, rowData []map[string]interface{}) []UpdateMessage {
	if len(rowData) % 2 != 0 {
		panic("update rows should be old/new pairs") // @FIXME that's pretty nasty
	}

	var ret []UpdateMessage
	var tmp map[string]interface{}

	for index,data := range rowData {
		if index % 2 == 0 {
			tmp = data
		} else {
			ret = append(ret, NewUpdateMessage(header, tmp, data))
		}
	}

	return ret
}

func CreateInsertMessagesFromRowData(header MessageHeader, rowData []map[string]interface{}) []InsertMessage {
	var ret []InsertMessage

	for _,data := range rowData {
		ret = append(ret, NewInsertMessage(header, data))
	}

	return ret
}

func CreateDeleteMessagesFromRowData(header MessageHeader, rowData []map[string]interface{}) []DeleteMessage {
	var ret []DeleteMessage

	for _,data := range rowData {
		ret = append(ret, NewDeleteMessage(header, data))
	}

	return ret
}

type TableMap struct {
	lookupMap map[uint64]TableMetadata
	db* sql.DB
}

func NewTableMap(db* sql.DB) TableMap {
	return TableMap{db: db, lookupMap: make(map[uint64]TableMetadata)}
}

func (m *TableMap) Add(id uint64, schema string, table string) {
	m.lookupMap[id] = TableMetadata{schema, table, GetFields(m.db, schema, table)}
}

func (m *TableMap) LookupTableMetadata(id uint64) (TableMetadata, bool) {
	val, ok := m.lookupMap[id]
	return val, ok
}


type EventBuffer struct {
	buffered []RowsEventData
}

func NewEventBuffer() EventBuffer {
	return EventBuffer{}
}

func (mb *EventBuffer) BufferRowsEventData(d RowsEventData) {
	mb.buffered = append(mb.buffered, d)
}

func (mb *EventBuffer) Drain() []RowsEventData {
	ret := mb.buffered
	mb.buffered = nil

	return ret
}


type RowsEventData struct {
	EventType replication.EventType
	RowData []map[string]interface{}
	HeaderBuilder MessageHeaderBuilder
}


func ConvertRowsEventsToMessages(xId uint64, rowsEventsData []RowsEventData) []Message {
	var ret []Message

	for _,d := range rowsEventsData {
		header := d.HeaderBuilder(xId)
		rowData := d.RowData

		// @FIXME warn for not handled event type
		switch d.EventType {
		case replication.WRITE_ROWS_EVENTv2:
			for _,message := range CreateInsertMessagesFromRowData(header, rowData) {
				ret = append(ret, Message(message))
			}

			break

		case replication.UPDATE_ROWS_EVENTv2:
			for _,message := range CreateUpdateMessagesFromRowData(header, rowData) {
				ret = append(ret, Message(message))
			}

			break

 		case replication.DELETE_ROWS_EVENTv2:
			for _,message := range CreateDeleteMessagesFromRowData(header, rowData) {
				ret = append(ret, Message(message))
			}

			break
		}
	}

	return ret
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

	eventBuffer := NewEventBuffer()
	tableMap := NewTableMap(db)

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
			}

			break

		case replication.XID_EVENT:
			xidEvent := e.Event.(*replication.XIDEvent)
			xId := uint64(xidEvent.XID)

			fmt.Fprintf(os.Stdout, ">>> ending transaction XID %d\n", xId)

			for _,message := range ConvertRowsEventsToMessages(xId, eventBuffer.Drain()) {
				b, err := json.MarshalIndent(message, "", "    ")

				if err != nil {
					fmt.Fprintf(os.Stdout, "JSON ERROR %s\n", err)
				} else {
					fmt.Fprintf(os.Stdout, "%s\n", b)
				}
			}

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

			rowData := RowData(rowsEvent, tableMetadata.fields)

			headerBuilder := NewMessageHeaderBuilder(
				tableMetadata.schema,
				tableMetadata.table,
				time.Unix(int64(e.Header.Timestamp), 0),
				e.Header.LogPos,
			)

			eventBuffer.BufferRowsEventData(RowsEventData{e.Header.EventType, rowData, headerBuilder})

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
