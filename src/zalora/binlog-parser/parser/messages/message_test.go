// +build unit

package messages

import (
	"testing"
	"time"
)

func TestNewMessageHeader(t *testing.T) {
	now := time.Now()
	binlogPosition := uint32(1)
	xid := uint64(2)

	messageHeader := NewMessageHeader("schema", "table", now, binlogPosition, xid)

	if messageHeader.Schema != "schema" {
		t.Fatal("Wrong schema in message header")
	}

	if messageHeader.Table != "table" {
		t.Fatal("Wrong table in message header")
	}

	if messageHeader.BinlogPosition != binlogPosition {
		t.Fatal("Wrong binlogPosition in message header")
	}

	if messageHeader.XId != xid {
		t.Fatal("Wrong Xid in message header")
	}
}
