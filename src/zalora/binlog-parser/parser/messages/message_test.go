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
	gtid := "3e11fa47-71ca-11e1-9e33-c80aa9429562:23"

	messageHeader := NewMessageHeader("schema", "table", now, binlogPosition, xid, gtid)

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
