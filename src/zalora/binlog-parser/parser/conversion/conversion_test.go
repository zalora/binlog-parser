// +build unit

package conversion

import (
	"fmt"
	"github.com/siddontang/go-mysql/replication"
	"reflect"
	"testing"
	"time"
	"zalora/binlog-parser/database"
	"zalora/binlog-parser/parser/messages"
)

func TestConvertQueryEventToMessage(t *testing.T) {
	logPos := uint32(100)
	query := "SELECT 1"

	eventHeader := replication.EventHeader{Timestamp: uint32(time.Now().Unix()), LogPos: logPos}
	queryEvent := replication.QueryEvent{Query: []byte(query)}

	message := ConvertQueryEventToMessage(eventHeader, queryEvent)

	assertMessageHeader(t, message, logPos, messages.MESSAGE_TYPE_QUERY)

	if string(message.(messages.QueryMessage).Query) != query {
		t.Fatal("Unexpected value for query ")
	}
}

func TestConvertRowsEventsToMessages(t *testing.T) {
	logPos := uint32(100)
	xId := uint64(200)

	tableMetadata := database.TableMetadata{"db_name", "table_name", map[int]string{0: "field_1", 1: "field_2"}}

	testCasesWriteRowsEvents := []struct {
		eventType replication.EventType
	}{
		{replication.WRITE_ROWS_EVENTv1},
		{replication.WRITE_ROWS_EVENTv2},
	}

	for _, tc := range testCasesWriteRowsEvents {
		t.Run("Insert message", func(t *testing.T) {
			eventHeader := createEventHeader(logPos, tc.eventType)
			rowsEvent := createRowsEvent([]interface{}{"value_1", "value_2"}, []interface{}{"value_3", "value_4"})
			rowsEventData := []RowsEventData{NewRowsEventData(eventHeader, rowsEvent, tableMetadata)}

			convertedMessages := ConvertRowsEventsToMessages(xId, rowsEventData)

			if len(convertedMessages) != 2 {
				t.Fatal("Expected 2 insert messages to be created")
			}

			assertMessageHeader(t, convertedMessages[0], logPos, messages.MESSAGE_TYPE_INSERT)
			assertMessageHeader(t, convertedMessages[1], logPos, messages.MESSAGE_TYPE_INSERT)

			insertMessageOne := convertedMessages[0].(messages.InsertMessage)

			if !reflect.DeepEqual(insertMessageOne.Data, messages.MessageRowData{Row: messages.MessageRow{"field_1": "value_1", "field_2": "value_2"}}) {
				t.Fatal(fmt.Sprintf("Wrong data for insert message 1 - got %v", insertMessageOne.Data))
			}

			insertMessageTwo := convertedMessages[1].(messages.InsertMessage)

			if !reflect.DeepEqual(insertMessageTwo.Data, messages.MessageRowData{Row: messages.MessageRow{"field_1": "value_3", "field_2": "value_4"}}) {
				t.Fatal(fmt.Sprintf("Wrong data for insert message 2 - got %v", insertMessageTwo.Data))
			}
		})
	}

	testCasesDeleteRowsEvents := []struct {
		eventType replication.EventType
	}{
		{replication.DELETE_ROWS_EVENTv1},
		{replication.DELETE_ROWS_EVENTv2},
	}

	for _, tc := range testCasesDeleteRowsEvents {
		t.Run("Delete message", func(t *testing.T) {
			eventHeader := createEventHeader(logPos, tc.eventType)
			rowsEvent := createRowsEvent([]interface{}{"value_1", "value_2"}, []interface{}{"value_3", "value_4"})
			rowsEventData := []RowsEventData{NewRowsEventData(eventHeader, rowsEvent, tableMetadata)}

			convertedMessages := ConvertRowsEventsToMessages(xId, rowsEventData)

			if len(convertedMessages) != 2 {
				t.Fatal("Expected 2 delete messages to be created")
			}

			assertMessageHeader(t, convertedMessages[0], logPos, messages.MESSAGE_TYPE_DELETE)
			assertMessageHeader(t, convertedMessages[1], logPos, messages.MESSAGE_TYPE_DELETE)

			deleteMessageOne := convertedMessages[0].(messages.DeleteMessage)

			if !reflect.DeepEqual(deleteMessageOne.Data, messages.MessageRowData{Row: messages.MessageRow{"field_1": "value_1", "field_2": "value_2"}}) {
				t.Fatal(fmt.Sprintf("Wrong data for delete message 1 - got %v", deleteMessageOne.Data))
			}

			deleteMessageTwo := convertedMessages[1].(messages.DeleteMessage)

			if !reflect.DeepEqual(deleteMessageTwo.Data, messages.MessageRowData{Row: messages.MessageRow{"field_1": "value_3", "field_2": "value_4"}}) {
				t.Fatal(fmt.Sprintf("Wrong data for delete message 2 - got %v", deleteMessageTwo.Data))
			}
		})
	}

	testCasesUpdateRowsEvents := []struct {
		eventType replication.EventType
	}{
		{replication.UPDATE_ROWS_EVENTv1},
		{replication.UPDATE_ROWS_EVENTv2},
	}

	for _, tc := range testCasesUpdateRowsEvents {
		t.Run("Update message", func(t *testing.T) {
			eventHeader := createEventHeader(logPos, tc.eventType)
			rowsEvent := createRowsEvent([]interface{}{"value_1", "value_2"}, []interface{}{"value_3", "value_4"})
			rowsEventData := []RowsEventData{NewRowsEventData(eventHeader, rowsEvent, tableMetadata)}

			convertedMessages := ConvertRowsEventsToMessages(xId, rowsEventData)

			if len(convertedMessages) != 1 {
				t.Fatal("Expected 1 update messages to be created")
			}

			assertMessageHeader(t, convertedMessages[0], logPos, messages.MESSAGE_TYPE_UPDATE)

			updateMessage := convertedMessages[0].(messages.UpdateMessage)

			if !reflect.DeepEqual(updateMessage.OldData, messages.MessageRowData{Row: messages.MessageRow{"field_1": "value_1", "field_2": "value_2"}}) {
				t.Fatal(fmt.Sprintf("Wrong data for update message old data - got %v", updateMessage.OldData))
			}

			if !reflect.DeepEqual(updateMessage.NewData, messages.MessageRowData{Row: messages.MessageRow{"field_1": "value_3", "field_2": "value_4"}}) {
				t.Fatal(fmt.Sprintf("Wrong data for update message new data - got %v", updateMessage.NewData))
			}
		})
	}

	t.Run("Unknown event type", func(t *testing.T) {
		eventHeader := createEventHeader(logPos, replication.RAND_EVENT) // can be any unkown event actually
		rowsEvent := createRowsEvent()
		rowsEventData := []RowsEventData{NewRowsEventData(eventHeader, rowsEvent, tableMetadata)}

		convertedMessages := ConvertRowsEventsToMessages(xId, rowsEventData)

		if len(convertedMessages) != 0 {
			t.Fatal("Expected no messages to be created from unknown event")
		}
	})
}

func createEventHeader(logPos uint32, eventType replication.EventType) replication.EventHeader {
	return replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		EventType: eventType,
		LogPos:    logPos,
	}
}

func createRowsEvent(rowData ...[]interface{}) replication.RowsEvent {
	return replication.RowsEvent{Rows: rowData}
}

func assertMessageHeader(t *testing.T, message messages.Message, expectedLogPos uint32, expectedType messages.MessageType) {
	if message.GetHeader().BinlogPosition != expectedLogPos {
		t.Fatal("Unexpected value for BinlogPosition")
	}

	if message.GetType() != expectedType {
		t.Fatal("Unexpected value for message type")
	}
}
