package conversion

import (
	"github.com/golang/glog"
	"github.com/siddontang/go-mysql/replication"
	"time"
	"zalora/binlog-parser/database"
	"zalora/binlog-parser/parser/messages"
)

type RowsEventData struct {
	BinlogEventHeader replication.EventHeader
	BinlogEvent       replication.RowsEvent
	TableMetadata     database.TableMetadata
}

func NewRowsEventData(binlogEventHeader replication.EventHeader, binlogEvent replication.RowsEvent, tableMetadata database.TableMetadata) RowsEventData {
	return RowsEventData{
		BinlogEventHeader: binlogEventHeader,
		BinlogEvent:       binlogEvent,
		TableMetadata:     tableMetadata,
	}
}

func ConvertQueryEventToMessage(binlogEventHeader replication.EventHeader, binlogEvent replication.QueryEvent) messages.Message {
	header := messages.NewMessageHeader(
		string(binlogEvent.Schema),
		"(unknown)",
		time.Unix(int64(binlogEventHeader.Timestamp), 0),
		binlogEventHeader.LogPos,
		0,
	)

	message := messages.NewQueryMessage(
		header,
		messages.SqlQuery(binlogEvent.Query),
	)

	return messages.Message(message)
}

func ConvertRowsEventsToMessages(xId uint64, rowsEventsData []RowsEventData) []messages.Message {
	var ret []messages.Message

	for _, d := range rowsEventsData {
		rowData := mapRowDataDataToColumnNames(d.BinlogEvent.Rows, d.TableMetadata.Fields)

		header := messages.NewMessageHeader(
			d.TableMetadata.Schema,
			d.TableMetadata.Table,
			time.Unix(int64(d.BinlogEventHeader.Timestamp), 0),
			d.BinlogEventHeader.LogPos,
			xId,
		)

		switch d.BinlogEventHeader.EventType {
		case replication.WRITE_ROWS_EVENTv1,
			replication.WRITE_ROWS_EVENTv2:
			for _, message := range createInsertMessagesFromRowData(header, rowData) {
				ret = append(ret, messages.Message(message))
			}

			break

		case replication.UPDATE_ROWS_EVENTv1,
			replication.UPDATE_ROWS_EVENTv2:
			for _, message := range createUpdateMessagesFromRowData(header, rowData) {
				ret = append(ret, messages.Message(message))
			}

			break

		case replication.DELETE_ROWS_EVENTv1,
			replication.DELETE_ROWS_EVENTv2:
			for _, message := range createDeleteMessagesFromRowData(header, rowData) {
				ret = append(ret, messages.Message(message))
			}

			break

		default:
			glog.Errorf("Can't convert unknown event %s", d.BinlogEventHeader.EventType)

			break
		}
	}

	return ret
}

func createUpdateMessagesFromRowData(header messages.MessageHeader, rowData []messages.MessageRowData) []messages.UpdateMessage {
	if len(rowData)%2 != 0 {
		panic("update rows should be old/new pairs") // should never happen as per mysql format
	}

	var ret []messages.UpdateMessage
	var tmp messages.MessageRowData

	for index, data := range rowData {
		if index%2 == 0 {
			tmp = data
		} else {
			ret = append(ret, messages.NewUpdateMessage(header, tmp, data))
		}
	}

	return ret
}

func createInsertMessagesFromRowData(header messages.MessageHeader, rowData []messages.MessageRowData) []messages.InsertMessage {
	var ret []messages.InsertMessage

	for _, data := range rowData {
		ret = append(ret, messages.NewInsertMessage(header, data))
	}

	return ret
}

func createDeleteMessagesFromRowData(header messages.MessageHeader, rowData []messages.MessageRowData) []messages.DeleteMessage {
	var ret []messages.DeleteMessage

	for _, data := range rowData {
		ret = append(ret, messages.NewDeleteMessage(header, data))
	}

	return ret
}
