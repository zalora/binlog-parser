package conversion

import (
	"time"
	"fmt"
	"zalora/binlog-parser/parser/messages"
	"zalora/binlog-parser/parser/database"
	"github.com/siddontang/go-mysql/replication"
)

type RowsEventData struct {
	BinlogEventHeader replication.EventHeader
	BinlogEvent replication.RowsEvent
	TableMetadata database.TableMetadata
}

func ConvertQueryEventToMessage(binlogEventHeader replication.EventHeader, binlogEvent replication.QueryEvent) messages.Message {
	header := messages.NewMinimalMessageHeader(time.Unix(int64(binlogEventHeader.Timestamp), 0), binlogEventHeader.LogPos)
	message := messages.NewQueryMessage(header, messages.SqlQuery(binlogEvent.Query))
	return messages.Message(message)
}

func ConvertRowsEventsToMessages(xId uint64, rowsEventsData []RowsEventData) []messages.Message {
	var ret []messages.Message

	for _,d := range rowsEventsData {
		rowData := rowData(d.BinlogEvent, d.TableMetadata.Fields)

		header := messages.NewMessageHeader(
			d.TableMetadata.Schema,
			d.TableMetadata.Table,
			time.Unix(int64(d.BinlogEventHeader.Timestamp), 0),
			d.BinlogEventHeader.LogPos,
			xId,
		)

		// @FIXME warn for not handled event type
		switch d.BinlogEventHeader.EventType {
		case replication.WRITE_ROWS_EVENTv2:
			for _,message := range createInsertMessagesFromRowData(header, rowData) {
				ret = append(ret, messages.Message(message))
			}

			break

		case replication.UPDATE_ROWS_EVENTv2:
			for _,message := range createUpdateMessagesFromRowData(header, rowData) {
				ret = append(ret, messages.Message(message))
			}

			break

 		case replication.DELETE_ROWS_EVENTv2:
			for _,message := range createDeleteMessagesFromRowData(header, rowData) {
				ret = append(ret, messages.Message(message))
			}

			break
		}
	}

	return ret
}

func createUpdateMessagesFromRowData(header messages.MessageHeader, rowData []map[string]interface{}) []messages.UpdateMessage {
	if len(rowData) % 2 != 0 {
		panic("update rows should be old/new pairs") // @FIXME that's pretty nasty
	}

	var ret []messages.UpdateMessage
	var tmp map[string]interface{}

	for index,data := range rowData {
		if index % 2 == 0 {
			tmp = data
		} else {
			ret = append(ret, messages.NewUpdateMessage(header, tmp, data))
		}
	}

	return ret
}

func createInsertMessagesFromRowData(header messages.MessageHeader, rowData []map[string]interface{}) []messages.InsertMessage {
	var ret []messages.InsertMessage

	for _,data := range rowData {
		ret = append(ret, messages.NewInsertMessage(header, data))
	}

	return ret
}

func createDeleteMessagesFromRowData(header messages.MessageHeader, rowData []map[string]interface{}) []messages.DeleteMessage {
	var ret []messages.DeleteMessage

	for _,data := range rowData {
		ret = append(ret, messages.NewDeleteMessage(header, data))
	}

	return ret
}

func rowData(rowsEvent replication.RowsEvent, columnNames map[int]string) []map[string]interface{} {
	var ret []map[string]interface{}

	for _, rows := range rowsEvent.Rows {
		var data map[string]interface{}
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
