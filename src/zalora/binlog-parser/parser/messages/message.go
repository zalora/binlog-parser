package messages

import (
	"time"
)

type MessageType string

const (
	MESSAGE_TYPE_INSERT MessageType = "Insert"
	MESSAGE_TYPE_UPDATE MessageType = "Update"
	MESSAGE_TYPE_DELETE MessageType = "Delete"
	MESSAGE_TYPE_QUERY MessageType = "Query"
)

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

func NewMinimalMessageHeader(binlogMessageTime time.Time, binlogPosition uint32) MessageHeader {
	return MessageHeader {
		BinlogMessageTime: binlogMessageTime.Format(time.RFC3339),
		MessageTime: time.Now().Format(time.RFC3339Nano),
		BinlogPosition: binlogPosition,
	}
}

type Message interface {}

type SqlQuery string

type QueryMessage struct {
	Header MessageHeader
	Type MessageType
	Query SqlQuery
}

func NewQueryMessage(header MessageHeader, query SqlQuery) QueryMessage {
	return QueryMessage{Header: header, Type: MESSAGE_TYPE_QUERY, Query: query}
}

type UpdateMessage struct {
	Header MessageHeader
	Type MessageType
	OldData map[string]interface{}
	NewData map[string]interface{}
}

func NewUpdateMessage(header MessageHeader, oldData map[string]interface{}, newData map[string]interface{}) UpdateMessage {
	return UpdateMessage{Header: header, Type: MESSAGE_TYPE_UPDATE, OldData: oldData, NewData: newData}
}

type InsertMessage struct {
	Header MessageHeader
	Type MessageType
	Data map[string]interface{}
}

func NewInsertMessage(header MessageHeader, data map[string]interface{}) InsertMessage {
	return InsertMessage{Header: header, Type: MESSAGE_TYPE_INSERT, Data: data}
}

type DeleteMessage struct {
	Header MessageHeader
	Type MessageType
	Data map[string]interface{}
}

func NewDeleteMessage(header MessageHeader, data map[string]interface{}) DeleteMessage {
	return DeleteMessage{Header: header, Type: MESSAGE_TYPE_DELETE, Data: data}
}
