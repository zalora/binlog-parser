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

	t.Run("Drain Empty", func(t *testing.T) {
		buffer := NewRowsEventBuffer()
		buffered := buffer.Drain()

		if len(buffered) != 0 {
			t.Fatal("Wrong number of entries retrieved from empty buffer")
		}
	})

	t.Run("Drain and re-fill", func(t *testing.T) {
		buffer := NewRowsEventBuffer()
		buffer.BufferRowsEventData(eventDataOne)
		buffer.BufferRowsEventData(eventDataTwo)

		buffered := buffer.Drain()

		if len(buffered) != 2 {
			t.Fatal("Wrong number of entries retrieved from buffer")
		}

		if !reflect.DeepEqual(buffered[0], eventDataOne) {
			t.Fatal("Retrieved wrong entry at index 0 from buffer")
		}

		if !reflect.DeepEqual(buffered[1], eventDataOne) {
			t.Fatal("Retrieved wrong entry at index 1 from buffer")
		}

		buffer.BufferRowsEventData(eventDataOne)

		buffered = buffer.Drain()

		if len(buffered) != 1 {
			t.Fatal("Wrong number of entries retrieved from re-used buffer")
		}

		if !reflect.DeepEqual(buffered[0], eventDataOne) {
			t.Fatal("Retrieved wrong entry at index 0 from re-used buffer")
		}
	})
}
