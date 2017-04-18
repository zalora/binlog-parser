package watcher

import (
//	"zalora/binlog-parser/filesystem"
)

func TrackBinlogFiles(binlogIndexFile string, indexFile string) {
	// index := index.NewIndex(indexFile)

	// binlogIndexChanged := func (err error) bool {
	// 	filesToParse := index.Diff(index.NewIndex(binlogIndexFile))

	// 	foreach filesToParse as f {
	// 		parser.ParseFile(f)
	// 		index.RecordParsedFile(f)
	// 	}

	// 	return true
	// }

	// filesystem.WatchFileChanges(binlogIndexFile, binlogIndexChanged)
}
