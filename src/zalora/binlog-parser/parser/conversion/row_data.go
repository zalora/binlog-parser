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

		for columnIndex, columnValue := range row {
			columnName, exists := columnNames[columnIndex]

			if !exists {
				columnName = fmt.Sprintf("(unknown_%d)", unknownCount)
				unknownCount++
			}

			data[columnName] = columnValue
		}

		if detected, mismatchNotice := detectMismatch(row, columnNames); detected {
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
