package database

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func GetDatabaseInstance(connectionString string) *sql.DB {
	db, db_err := sql.Open("mysql", connectionString)

	if db_err != nil {
		panic(db_err.Error())
	}

	db_err = db.Ping()

	if db_err != nil {
		panic(db_err.Error())
	}

	return db
}
