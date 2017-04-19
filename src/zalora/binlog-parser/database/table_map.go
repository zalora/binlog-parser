package database

import (
	"database/sql"
)

type TableMetadata struct {
	Schema string
	Table  string
	Fields map[int]string
}

type TableMap struct {
	lookupMap map[uint64]TableMetadata
	db        *sql.DB
}

func NewTableMap(db *sql.DB) TableMap {
	return TableMap{db: db, lookupMap: make(map[uint64]TableMetadata)}
}

func (m *TableMap) Add(id uint64, schema string, table string) error {
	fields, err := getFields(m.db, schema, table)

	if err != nil {
		return err
	}

	m.lookupMap[id] = TableMetadata{schema, table, fields}

	return nil
}

func (m *TableMap) LookupTableMetadata(id uint64) (TableMetadata, bool) {
	val, ok := m.lookupMap[id]
	return val, ok
}

func getFields(db *sql.DB, schema string, table string) (map[int]string, error) {
	rows, err := db.Query(
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		schema,
		table,
	)

	if err != nil {
		q := newQueryError(err)
		return nil, &q
	}

	defer rows.Close()

	fields := make(map[int]string)
	i := 0

	var columnName string
	for rows.Next() {
		err := rows.Scan(&columnName)

		if err != nil {
			q := newQueryError(err)
			return nil, &q
		}

		fields[i] = columnName
		i++
	}

	return fields, nil
}
