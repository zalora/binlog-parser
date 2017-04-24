package conversion

import (
	"fmt"
	"zalora/binlog-parser/parser/messages"
)

func mapRowDataDataToColumnNames(rows [][]interface{}, columnNames map[int]string) []messages.MessageRowData {
	var mappedRows []messages.MessageRowData

	for _, row := range rows {
		data := make(map[string]interface{})
		unknownCount := 0

		detectedMismatch, mismatchNotice := detectMismatch(row, columnNames)

		for columnIndex, columnValue := range row {
			if detectedMismatch {
				data[fmt.Sprintf("(unknown_%d)", unknownCount)] = columnValue
				unknownCount++
			} else {
				columnName, exists := columnNames[columnIndex]

				if !exists {
					// This should actually never happen
					// Fail hard before doing anything weird
					panic(fmt.Sprintf("No mismatch between row and column names array detected, but column %s not found", columnName))
				}

				data[columnName] = columnValue
			}
		}

		if detectedMismatch {
			mappedRows = append(mappedRows, messages.MessageRowData{Row: data, MappingNotice: mismatchNotice})
		} else {
			mappedRows = append(mappedRows, messages.MessageRowData{Row: data})
		}
	}

	return mappedRows
}

func detectMismatch(row []interface{}, columnNames map[int]string) (bool, string) {
	if len(row) > len(columnNames) {
		return true, fmt.Sprintf("column names array is missing field(s), will map them as unknown_*")
	}

	if len(row) < len(columnNames) {
		return true, fmt.Sprintf("row is missing field(s), ignoring missing")
	}

	return false, ""
}
