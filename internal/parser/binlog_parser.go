package parser

import (
	"0michalsokolowski0/binlog-parser/internal/database"
	"0michalsokolowski0/binlog-parser/internal/parser/parser"
	"os"
)

func ParseBinlog(binlogFilename string, tableMap database.TableMap, consumerChain ConsumerChain) error {
	if _, err := os.Stat(binlogFilename); os.IsNotExist(err) {
		return err
	}

	return parser.ParseBinlogToMessages(binlogFilename, tableMap, consumerChain.consumeMessage)
}
