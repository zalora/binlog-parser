package database

import (
	"database/sql"
)

type TableMetadata struct {
	Schema string
	Table string
	Fields map[int]string
}

type TableMap struct {
	lookupMap map[uint64]TableMetadata
	db* sql.DB
}

func NewTableMap(db* sql.DB) TableMap {
	return TableMap{db: db, lookupMap: make(map[uint64]TableMetadata)}
}

func (m *TableMap) Add(id uint64, schema string, table string) {
	m.lookupMap[id] = TableMetadata{schema, table, getFields(m.db, schema, table)}
}

func (m *TableMap) LookupTableMetadata(id uint64) (TableMetadata, bool) {
	val, ok := m.lookupMap[id]
	return val, ok
}

func getFields(db* sql.DB, schema string, table string) map[int]string {
	rows, db_err := db.Query(
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		schema,
		table,
	)

	if db_err != nil {
		panic(db_err.Error()) // @FIXME proper error handling
	}

	defer rows.Close()

	fields := make(map[int]string)
	i := 0

	var columnName string
	for rows.Next() {
		db_err := rows.Scan(&columnName)

		if db_err != nil {
			panic(db_err.Error()) // @FIXME proper error handling
		}

		fields[i] = columnName
		i++;
	}

	return fields
}
