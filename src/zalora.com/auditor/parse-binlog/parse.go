package main

import (
	"encoding/json"
	"time"
	"github.com/siddontang/go-mysql/replication"
	"os"
	"fmt"
	"strings"
	"database/sql"
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
	TransactionId uint64
}

func NewMessageHeader(schema string, table string, binlogMessageTime time.Time, binlogPosition uint32, transactionId uint64) MessageHeader {
	return MessageHeader {
		Schema: schema,
		Table: table,
		BinlogMessageTime: binlogMessageTime.Format(time.RFC3339),
		MessageTime: time.Now().Format(time.RFC3339Nano),
		BinlogPosition: binlogPosition,
		TransactionId: transactionId,
	}
}

type MessageHeaderBuilder func(uint64) MessageHeader

func NewMessageHeaderBuilder(schema string, table string, binlogMessageTime time.Time, binlogPosition uint32) MessageHeaderBuilder {
	return func (transactionId uint64) MessageHeader {
		return NewMessageHeader(schema, table, binlogMessageTime, binlogPosition, transactionId)
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

func NewUpdateMessageAsGenericMessageBuilder(headerBuilderFunc MessageHeaderBuilder, oldData map[string]interface{}, newData map[string]interface{}) MessageBuilder {
	return func (transactionId uint64) Message {
		return Message(NewUpdateMessage(headerBuilderFunc(transactionId), oldData, newData))
	}
}

type InsertMessage struct {
	Header MessageHeader
	Type string
	Data map[string]interface{}
}

func NewInsertMessage(header MessageHeader, data map[string]interface{}) InsertMessage {
	return InsertMessage{Header: header, Type: "INSERT", Data: data}
}


func NewInsertMessageAsGenericMessageBuilder(headerBuilderFunc MessageHeaderBuilder, data map[string]interface{}) MessageBuilder {
	return func (transactionId uint64) Message {
		return Message(NewInsertMessage(headerBuilderFunc(transactionId), data))
	}
}

type DeleteMessage struct {
	Header MessageHeader
	Type string
	Data map[string]interface{}
}

func NewDeleteMessage(header MessageHeader, data map[string]interface{}) DeleteMessage {
	return DeleteMessage{Header: header, Type: "DELETE", Data: data}
}


func NewDeleteMessageAsGenericMessageBuilder(headerBuilderFunc MessageHeaderBuilder, data map[string]interface{}) MessageBuilder {
	return func (transactionId uint64) Message {
		return Message(NewDeleteMessage(headerBuilderFunc(transactionId), data))
	}
}

type MessageBuildersFromRowDataFunc func (headerBuilder MessageHeaderBuilder, rowData []map[string]interface{}) []MessageBuilder


func CreateUpdateMessageBuildersFromRowData(headerBuilder MessageHeaderBuilder, rowData []map[string]interface{}) []MessageBuilder {
	if len(rowData) % 2 != 0 {
		panic("update rows should be old/new pairs") // @FIXME that's pretty nasty
	}

	var ret []MessageBuilder

	var tmp map[string]interface{}

	for index,element := range rowData {
		if index % 2 == 0 {
			tmp = element
		} else {
			messageBuilder := NewUpdateMessageAsGenericMessageBuilder(headerBuilder, tmp, element)
			ret = append(ret, messageBuilder)
		}
	}

	return ret
}

func CreateInsertMessageBuildersFromRowData(headerBuilder MessageHeaderBuilder, rowData []map[string]interface{}) []MessageBuilder {
	var ret []MessageBuilder

	for _,element := range rowData {
		messageBuilder := NewInsertMessageAsGenericMessageBuilder(headerBuilder, element)

		ret = append(ret, messageBuilder)
	}

	return ret
}

func CreateDeleteMessageBuildersFromRowData(headerBuilder MessageHeaderBuilder, rowData []map[string]interface{}) []MessageBuilder {
	var ret []MessageBuilder

	for _,element := range rowData {
		messageBuilder := NewDeleteMessageAsGenericMessageBuilder(headerBuilder, element)

		ret = append(ret, messageBuilder)
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

type MessageBuffer struct {
	buffered []MessageBuilder
}

func NewMessageBuffer() MessageBuffer {
	return MessageBuffer{}
}

func (mb *MessageBuffer) BufferMessageBuilder(messageBuilder ...MessageBuilder) {
	mb.buffered = append(mb.buffered, messageBuilder...)
}

func (mb *MessageBuffer) BuildAllMessagesForTransactionId(transactionId uint64) []Message {
	fmt.Fprintf(os.Stdout, "### Getting ALL for txid %d\n", transactionId)

	var ret []Message

	for _,messageBuilder := range mb.buffered {
		ret = append(ret, messageBuilder(transactionId))
	}

	mb.buffered = nil

	return ret
}

func HandleRowDataEvent(
	e *replication.BinlogEvent,
	messageBuilderFromRowDataFunc MessageBuildersFromRowDataFunc,
	tableMap *TableMap,
	messageBuffer *MessageBuffer,
) {
	rowsEvent := e.Event.(*replication.RowsEvent)
	tableId := uint64(rowsEvent.TableID)

	tableMetadata, ok := tableMap.LookupTableMetadata(tableId)

	if ok == false {
		// @TODO handle this
		fmt.Fprintf(os.Stdout, "@@@ ERROR table info NOT FOUND\n")
		return
	}

	rowData := RowData(rowsEvent, tableMetadata.fields)
	messageBuilders := messageBuilderFromRowDataFunc(
		NewMessageHeaderBuilder(tableMetadata.schema, tableMetadata.table, time.Unix(int64(e.Header.Timestamp), 0), e.Header.LogPos),
		rowData,
	)

	messageBuffer.BufferMessageBuilder(messageBuilders...)
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

	messageBuffer := NewMessageBuffer()
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
			xid := uint64(xidEvent.XID)

			fmt.Fprintf(os.Stdout, ">>> ending transaction XID %d\n", xid)

			for _,message := range messageBuffer.BuildAllMessagesForTransactionId(xid) {
				b, err := json.MarshalIndent(message, "", "    ")

				if err != nil {
					fmt.Fprintf(os.Stdout, "JSON FAIL %s\n", err)
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
			fmt.Fprintf(os.Stdout, ">>> INSERT event\n")
			messageBuildersFromRowDataFunc := CreateInsertMessageBuildersFromRowData
			HandleRowDataEvent(e, messageBuildersFromRowDataFunc, &tableMap, &messageBuffer)

			break

		case replication.UPDATE_ROWS_EVENTv2:
			fmt.Fprintf(os.Stdout, ">>> UPDATE event\n")
			messageBuildersFromRowDataFunc := CreateUpdateMessageBuildersFromRowData
			HandleRowDataEvent(e, messageBuildersFromRowDataFunc, &tableMap, &messageBuffer)

			break

 		case replication.DELETE_ROWS_EVENTv2:
			fmt.Fprintf(os.Stdout, ">>> DELETE event\n")
			messageBuildersFromRowDataFunc := CreateDeleteMessageBuildersFromRowData
			HandleRowDataEvent(e, messageBuildersFromRowDataFunc, &tableMap, &messageBuffer)

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




// 		switch e.Header.EventType {
// 		// @TODO add query message
// 		case replication.QUERY_EVENT:
// 			queryEvent := e.Event.(*replication.QueryEvent)
// 			query := string(queryEvent.Query)

// 			// if strings.ToUpper(strings.Trim(query, " ")) == "BEGIN" {
// 			// }
// 			// xid := uint64(xidEvent.XID)

// 			fmt.Fprintf(os.Stdout, "POS at %d \n", e.Header.LogPos)
// 			fmt.Fprintf(os.Stdout, "query %s ###\n", query)

// 			break;

// 		case replication.XID_EVENT:
// 			xidEvent := e.Event.(*replication.XIDEvent)
// 			xid := uint64(xidEvent.XID)

// //			transactionLog.GetAll(xid)

// 			fmt.Fprintf(os.Stdout, "POS at %d \n", e.Header.LogPos)
// 			fmt.Fprintf(os.Stdout, "XID %d ###\n", xid)

// 			break;

// 		case replication.UPDATE_ROWS_EVENTv2:
// 			updateRowsEvent := e.Event.(*replication.RowsEvent)
// 			tableId := uint64(updateRowsEvent.TableID)

// 			// let's check only bob tables
// 			if InterestingTable(tableMap[tableId]) == false {
// 				break
// 			}

// 			columnNames := GetFields(db, "test_db", tableMap[tableId].b)
// 			rowData := RowData(updateRowsEvent, columnNames)

// 			for _,element := range CreateUpdateMessageBuilders(tableMap[tableId], time.Unix(int64(e.Header.Timestamp), 0), e.Header.LogPos, rowData) {
// 				b, err := json.MarshalIndent(element, "", "    ")

// 				if err != nil {
// 					fmt.Fprintf(os.Stdout, "%s\n", err)
// 				} else {
// 					fmt.Fprintf(os.Stdout, "%s\n", b)
// 				}
// 			}

// 			break;

// 		case replication.WRITE_ROWS_EVENTv2:
// 			writeRowsEvent := e.Event.(*replication.RowsEvent)
// 			tableId := uint64(writeRowsEvent.TableID)

// 			// let's check only bob tables
// 			if InterestingTable(tableMap[tableId]) == false {
// 				break
// 			}

// 			columnNames := GetFields(db, "test_db", tableMap[tableId].b)
// 			rowData := RowData(writeRowsEvent, columnNames)

// 			for _,element := range CreateInsertMessageBuilders(tableMap[tableId], time.Unix(int64(e.Header.Timestamp), 0), e.Header.LogPos, rowData) {
// 				_, err := json.MarshalIndent(element, "", "    ")

// 				if err != nil {
// 					fmt.Fprintf(os.Stdout, "%s\n", err)
// 				} else {
// 					fmt.Fprintf(os.Stdout, "XXX outputting message\n")

// 					//fmt.Fprintf(os.Stdout, "XXX INSERT MESSAGE:\n%s\n", b)
// 				}
// 			}

// 			fmt.Fprintf(os.Stdout, "XXX ==============\n")

// 			break;

// 		case replication.DELETE_ROWS_EVENTv2:
// 			deleteRowsEvent := e.Event.(*replication.RowsEvent)
// 			tableId := uint64(deleteRowsEvent.TableID)

// 			// let's check only bob tables
// 			if InterestingTable(tableMap[tableId]) == false {
// 				break
// 			}

// 			// get bob table fields from our local test_db
// 			columnNames := GetFields(db, "test_db", tableMap[tableId].b)
// 			rowData := RowData(deleteRowsEvent, columnNames)

// 			for _,element := range CreateDeleteMessageBuilders(tableMap[tableId], time.Unix(int64(e.Header.Timestamp), 0), e.Header.LogPos, rowData) {
// 				b, err := json.MarshalIndent(element, "", "    ")

// 				if err != nil {
// 					fmt.Fprintf(os.Stdout, "%s\n", err)
// 				} else {
// 					fmt.Fprintf(os.Stdout, "%s\n", b)
// 				}
// 			}

// 			break;

// 		case replication.TABLE_MAP_EVENT:
// 			tableMapEvent := e.Event.(*replication.TableMapEvent)
// 			schema := string(tableMapEvent.Schema)
// 			table := string(tableMapEvent.Table)
// 			tableId := uint64(tableMapEvent.TableID)

// 			tableMap[tableId] = TableMetadata{schema, table}

// 			break;

// 		default:
// 			break;
// 		}
