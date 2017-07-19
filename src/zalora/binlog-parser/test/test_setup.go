package test

import (
	"os"
	//	"flag"
	"testing"
)

const TEST_DB_CONNECTION_STRING string = "root@/test_db"

func Setup(m *testing.M) {
	//	flag.Set("alsologtostderr", "true")
	//	flag.Set("v", "5")

	os.Exit(m.Run())
}
