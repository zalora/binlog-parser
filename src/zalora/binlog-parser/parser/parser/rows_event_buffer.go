package parser

import (
	"zalora/binlog-parser/parser/conversion"
)

type RowsEventBuffer struct {
	buffered []conversion.RowsEventData
	gtid string
}

func NewRowsEventBuffer() RowsEventBuffer {
	return RowsEventBuffer{}
}

func (mb *RowsEventBuffer) BufferRowsEventData(d conversion.RowsEventData) {
	mb.buffered = append(mb.buffered, d)
}

func (mb *RowsEventBuffer) SetGTID(gtid string) {
	mb.gtid = gtid
}

func (mb *RowsEventBuffer) GTID() string {
	return mb.gtid
}

func (mb *RowsEventBuffer) Drain() ([]conversion.RowsEventData, string) {
	events := mb.buffered
	gtid := mb.gtid

	mb.buffered = nil
	mb.gtid = ""

	return events, gtid
}
