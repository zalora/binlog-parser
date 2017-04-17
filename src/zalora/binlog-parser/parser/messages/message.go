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
	BinlogPosition uint32
	XId uint64
}

func NewMessageHeader(schema string, table string, binlogMessageTime time.Time, binlogPosition uint32, xId uint64) MessageHeader {
	return MessageHeader {
		Schema: schema,
		Table: table,
		BinlogMessageTime: binlogMessageTime.UTC().Format(time.RFC3339),
		BinlogPosition: binlogPosition,
		XId: xId,
	}
}

func NewMinimalMessageHeader(binlogMessageTime time.Time, binlogPosition uint32) MessageHeader {
	return MessageHeader {
		BinlogMessageTime: binlogMessageTime.UTC().Format(time.RFC3339),
		BinlogPosition: binlogPosition,
	}
}

type Message interface {
    GetHeader() MessageHeader
    GetType() MessageType
}

type baseMessage struct {
    Header MessageHeader
    Type MessageType
}

func (b baseMessage) GetHeader() MessageHeader {
    return b.Header
}

func (b baseMessage) GetType() MessageType {
    return b.Type
}

type SqlQuery string

type QueryMessage struct {
	baseMessage
	Query SqlQuery
}

func NewQueryMessage(header MessageHeader, query SqlQuery) QueryMessage {
	return QueryMessage{baseMessage: baseMessage{Header: header, Type: MESSAGE_TYPE_QUERY}, Query: query}
}

type UpdateMessage struct {
	baseMessage
	OldData map[string]interface{}
	NewData map[string]interface{}
}

func NewUpdateMessage(header MessageHeader, oldData map[string]interface{}, newData map[string]interface{}) UpdateMessage {
	return UpdateMessage{baseMessage: baseMessage{Header: header, Type: MESSAGE_TYPE_UPDATE}, OldData: oldData, NewData: newData}
}

type InsertMessage struct {
	baseMessage
	Data map[string]interface{}
}

func NewInsertMessage(header MessageHeader, data map[string]interface{}) InsertMessage {
	return InsertMessage{baseMessage: baseMessage{Header: header, Type: MESSAGE_TYPE_INSERT}, Data: data}
}

type DeleteMessage struct {
	baseMessage
	Data map[string]interface{}
}

func NewDeleteMessage(header MessageHeader, data map[string]interface{}) DeleteMessage {
	return DeleteMessage{baseMessage: baseMessage{Header: header, Type: MESSAGE_TYPE_DELETE}, Data: data}
}
