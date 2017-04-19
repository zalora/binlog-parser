package parser

import (
	"zalora/binlog-parser/database"
	"zalora/binlog-parser/parser/parser"
)

func ParseBinlog(binlogFileName string, tableMap database.TableMap, consumerChain ConsumerChain) error {
	return parser.ParseBinlogToMessages(binlogFileName, tableMap, consumerChain.consumeMessage)
}
