package filesystem

import (
	"github.com/fsnotify/fsnotify"
	"github.com/golang/glog"
)

type WatcherFunc func() error

func WatchDirChanges(dirname string, watcherFunc WatcherFunc) error {
	watcher, err := fsnotify.NewWatcher()

	if err != nil {
		glog.Errorf("Got error creating watcher %s", err)
	}

	defer watcher.Close()

	// call func at least once when attaching
	err = watcherFunc()

	if err != nil {
		glog.Errorf("Got error calling watcher func initially %s", err)
		return err
	}

	done := make(chan bool)

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				glog.Infof("inotify event %s", event)

				watcherErr := watcherFunc()

				if watcherErr != nil {
					done <- true
				}

			case err := <-watcher.Errors:
				glog.Errorf("Got error %s while watching, calling watcher func", err)

				done <- true
			}
		}
	}()

	glog.Infof("Start watching dir %s", dirname)

	err = watcher.Add(dirname)

	if err != nil {
		glog.Errorf("Got error adding watcher %s", err)
		return err
	}
	<-done
	return nil
}
