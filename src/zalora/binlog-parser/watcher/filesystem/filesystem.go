package filesystem

import (

	"github.com/golang/glog"
	"github.com/fsnotify/fsnotify"
)

type WatcherFunc func(err error) bool

func WatchFileChanges(filename string, watcherFunc WatcherFunc) {
	watcher, err := fsnotify.NewWatcher()

	if err != nil {
		glog.Errorf("Got error creating watcher %s", err)
	}

	defer watcher.Close()

	done := make(chan bool)

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				glog.Infof("inotify event %s", event)

				if event.Op&fsnotify.Write == fsnotify.Write {
					glog.Infof("modified file %s, calling watcher func", event.Name)

					if !watcherFunc(nil) {
						close(done)
					}
				}

			case err := <-watcher.Errors:
				glog.Errorf("Got error watching %s, calling watcher func", err)

				if !watcherFunc(err) {
					close(done)
				}
			}
		}
	}()

	glog.Infof("Start watching file %s", filename)

	err = watcher.Add(filename)

	if err != nil {
		glog.Errorf("Got error adding watcher %s", err)
	}
	<-done
}
