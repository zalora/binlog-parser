package database

import (
	"database/sql"
	"fmt"
)

type TableMetadata struct {
	Schema string
	Table  string
	Fields map[int]string
}

type TableMap struct {
	tableMetadataMap map[uint64]TableMetadata
	fieldsCache      map[string]map[int]string
	db               *sql.DB
}

func NewTableMap(db *sql.DB) TableMap {
	return TableMap{
		db:               db,
		tableMetadataMap: make(map[uint64]TableMetadata),
		fieldsCache:      make(map[string]map[int]string),
	}
}

func (m *TableMap) Add(id uint64, schema, table string) error {
	fields, err := m.getFields(schema, table)

	if err != nil {
		return err
	}

	m.tableMetadataMap[id] = TableMetadata{schema, table, fields}

	return nil
}

func (m *TableMap) LookupTableMetadata(id uint64) (TableMetadata, bool) {
	val, ok := m.tableMetadataMap[id]
	return val, ok
}

func (m *TableMap) getFields(schema, table string) (map[int]string, error) {
	cacheKey := fmt.Sprintf("%s_%s", schema, table)

	if cachedFields, ok := m.fieldsCache[cacheKey]; ok {
		return cachedFields, nil
	}

	fields, err := getFieldsFromDb(m.db, schema, table)
	m.fieldsCache[cacheKey] = fields

	if err != nil {
		return nil, err
	}

	return fields, nil
}

func getFieldsFromDb(db *sql.DB, schema string, table string) (map[int]string, error) {
	rows, err := db.Query(
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
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
