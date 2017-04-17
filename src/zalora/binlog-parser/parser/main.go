package main

// import (
// 	"encoding/json"
// 	"os"
// 	"fmt"
// 	"database/sql"
// 	"zalora/binlog-parser/parser/messages"
// 	"zalora/binlog-parser/database"
// 	_ "github.com/go-sql-driver/mysql"
// )

// func dumpMessage(message messages.Message) {
// 	for _,message := range messages {
// 		b, err := json.MarshalIndent(message, "", "    ")

// 		if err != nil {
// 			fmt.Fprintf(os.Stdout, "JSON ERROR %s\n", err)
// 		} else {
// 			fmt.Fprintf(os.Stdout, "%s\n", b)
// 		}
// 	}
// }

func main() {
	// // db connection
	// db, db_err := sql.Open("mysql", "root@/test_db")

	// if db_err != nil {
	// 	panic(db_err.Error()) // @FIXME proper error handling
	// }

	// defer db.Close()

	// db_err = db.Ping()

	// if db_err != nil {
	// 	panic(db_err.Error()) // @FIXME proper error handling
	// }

	// binlogFileName := os.Args[1]
	// offset := os.Args[2]

	// tableMap := database.NewTableMap(db)

	// parser.ParseBinlogToMessages(binlogFileName, offset, tab)
}
