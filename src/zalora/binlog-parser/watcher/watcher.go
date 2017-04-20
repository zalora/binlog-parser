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

type ParseFunc func(string) error

func WatchBinlogIndexFile(binlogIndexFilename string, watcherIndexFilename string, parseFunc ParseFunc) error {
	watcherFunc := createWatchFunc(binlogIndexFilename, watcherIndexFilename, parseFunc)
	return filesystem.WatchDirChanges(filepath.Dir(binlogIndexFilename), watcherFunc)
}

func createWatchFunc(binlogIndexFilename string, watcherIndexFilename string, parseFunc ParseFunc) func() error {
	return func() error {
		fileMutex.Lock()
		defer fileMutex.Unlock()
		glog.V(1).Info("binlog dir changed")

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
			glog.V(1).Info("No binlogs to parse")
			return nil
		}

		parsedFiles, err := parseFiles(filesToParse, parseFunc)

		if err != nil {
			return err
		}

		watcherIndex.Append(parsedFiles...)
		err = watcherIndex.Sync()

		if err != nil {
			return err
		}

		return nil
	}
}

func parseFiles(filesToParse []string, parseFunc ParseFunc) ([]string, error) {
	glog.V(1).Infof("Diff result: %v", filesToParse)

	var parsedFiles []string

	for _, line := range filesToParse[:len(filesToParse)-1] { // skip newest
		glog.V(1).Infof("Need to parse binlog %s", line)

		err := parseFunc(line)

		if err != nil {
			glog.V(1).Infof("Failed to parse binlog %s", line)

			return nil, err
		}

		glog.V(1).Infof("Successfully parsed binlog %s", line)

		parsedFiles = append(parsedFiles, line)
	}

	return parsedFiles, nil
}
