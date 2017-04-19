package watcher

import (
	"github.com/golang/glog"
	"os"
	"path/filepath"
	"sync"
	"zalora/binlog-parser/watcher/filesystem"
	"zalora/binlog-parser/watcher/index"
)

var fileMutex sync.Mutex

type ParseFunc func(string) (bool, error)

func WatchBinlogIndexFile(binlogIndexFilename string, watcherIndexFilename string, parseFunc ParseFunc) error {
	binlogIndexChanged := func() error {
		fileMutex.Lock()
		defer fileMutex.Unlock()

		glog.Info("binlog dir changed")

		watcherIndexFile, err := os.OpenFile(watcherIndexFilename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)

		if err != nil {
			glog.Errorf("Got error when creating watcher index: %s", err)
			return err
		}

		defer watcherIndexFile.Close()
		watcherIndex := index.NewIndex(watcherIndexFile)

		binlogIndexFile, err := os.Open(binlogIndexFilename)

		if err != nil {
			glog.Errorf("Got error when creating binlog index: %s", err)
			return err
		}

		defer binlogIndexFile.Close()
		binlogIndex := index.NewIndex(binlogIndexFile)

		filesToParse := binlogIndex.Diff(watcherIndex)

		if len(filesToParse) <= 1 {
			glog.Info("No binlogs to parse")
			return nil
		}

		glog.Infof("Diff result: %v", filesToParse)

		for _, line := range filesToParse[:len(filesToParse)-1] { // skip newest
			glog.Infof("Need to parse binlog %s", line)

			success, err := parseFunc(line)

			if err != nil {
				glog.Infof("Failed to parse binlog %s", line)

				return err
			}

			if success {
				watcherIndex.Append(line)
			}
		}

		watcherIndex.Sync()

		return nil
	}

	return filesystem.WatchDirChanges(filepath.Dir(binlogIndexFilename), binlogIndexChanged)
}
