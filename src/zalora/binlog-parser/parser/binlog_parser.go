package parser

import (
	"os"
	"zalora/binlog-parser/database"
	"zalora/binlog-parser/parser/parser"
)

func ParseBinlog(binlogFileName string, tableMap database.TableMap, consumerChain ConsumerChain) error {
	if _, err := os.Stat(binlogFileName); os.IsNotExist(err) {
		return err
	}

	return parser.ParseBinlogToMessages(binlogFileName, tableMap, consumerChain.consumeMessage)
}
