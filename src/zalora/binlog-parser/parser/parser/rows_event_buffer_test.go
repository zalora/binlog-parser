// +build unit

package parser

import (
	"reflect"
	"testing"
	"zalora/binlog-parser/parser/conversion"
)

func TestRowsEventBuffer(t *testing.T) {
	eventDataOne := conversion.RowsEventData{}
	eventDataTwo := conversion.RowsEventData{}
	gtidOne := "3e11fa47-71ca-11e1-9e33-c80aa9429562:23"
	gtidTwo := "3e11fa47-71ca-11e1-9e33-c80aa9429562:24"

	t.Run("Drain Empty", func(t *testing.T) {
		buffer := NewRowsEventBuffer()
		buffered, gtid := buffer.Drain()

		if len(buffered) != 0 {
			t.Fatal("Wrong number of entries retrieved from empty buffer")
		}

		if gtid != "" {
			t.Fatalf("Expected GTID to be empty, got: %v", gtid)
		}
	})

	t.Run("Drain and re-fill", func(t *testing.T) {
		buffer := NewRowsEventBuffer()
		buffer.BufferRowsEventData(eventDataOne)
		buffer.BufferRowsEventData(eventDataTwo)
		buffer.SetGTID(gtidOne)

		buffered, gtid := buffer.Drain()

		if len(buffered) != 2 {
			t.Fatal("Wrong number of entries retrieved from buffer")
		}

		if !reflect.DeepEqual(buffered[0], eventDataOne) {
			t.Fatal("Retrieved wrong entry at index 0 from buffer")
		}

		if !reflect.DeepEqual(buffered[1], eventDataOne) {
			t.Fatal("Retrieved wrong entry at index 1 from buffer")
		}

		if gtid != gtidOne {
			t.Fatalf("Expected GTID to be %v, got: %v", gtidOne, gtid)
		}

		buffer.BufferRowsEventData(eventDataOne)
		buffer.SetGTID(gtidTwo)

		buffered, gtid = buffer.Drain()

		if len(buffered) != 1 {
			t.Fatal("Wrong number of entries retrieved from re-used buffer")
		}

		if !reflect.DeepEqual(buffered[0], eventDataOne) {
			t.Fatal("Retrieved wrong entry at index 0 from re-used buffer")
		}

		if gtid != gtidTwo {
			t.Fatalf("Expected GTID to be %v, got: %v", gtidTwo, gtid)
		}
	})
}
