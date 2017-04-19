package database

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func GetDatabaseInstance(connectionString string) (*sql.DB, error) {
	db, err := sql.Open("mysql", connectionString)

	if err != nil {
		c := newConnectionError(err)
		return nil, &c
	}

	err = db.Ping()

	if err != nil {
		c := newConnectionError(err)
		return nil, &c
	}

	return db, nil
}
