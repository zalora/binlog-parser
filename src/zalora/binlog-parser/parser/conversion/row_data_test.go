package conversion

import (
	"strings"
	"testing"
)

func TestDetectMismatch(t *testing.T) {
	t.Run("No mismatch, empty input", func(t *testing.T) {
		row := []interface{}{}
		columnNames := map[int]string{}

		detected, _ := detectMismatch(row, columnNames)

		if detected {
			t.Fatal("Expected no mismatch to be detected")
		}
	})

	t.Run("No mismatch", func(t *testing.T) {
		row := []interface{}{"value 1", "value 2"}
		columnNames := map[int]string{0: "field_1", 1: "field_2"}

		detected, _ := detectMismatch(row, columnNames)

		if detected {
			t.Fatal("Expected no mismatch to be detected")
		}
	})

	t.Run("Detect mismatch, row is missing field", func(t *testing.T) {
		row := []interface{}{"value 1"}
		columnNames := map[int]string{0: "field_1", 1: "field_2"}

		detected, notice := detectMismatch(row, columnNames)

		if !detected {
			t.Fatal("Expected mismatch to be detected")
		}

		if !strings.Contains(notice, "row is missing field(s)") {
			t.Fatal("Wrong notice")
		}
	})

	t.Run("Detect mismatch, column name is missing field", func(t *testing.T) {
		row := []interface{}{"value 1", "value 2"}
		columnNames := map[int]string{0: "field_1"}

		detected, notice := detectMismatch(row, columnNames)

		if !detected {
			t.Fatal("Expected mismatch to be detected")
		}

		if !strings.Contains(notice, "column names array is missing field(s)") {
			t.Fatal("Wrong notice")
		}
	})
}
