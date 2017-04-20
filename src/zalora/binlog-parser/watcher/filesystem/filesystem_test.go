package filesystem

import (
	"errors"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestWatchDirChanges(t *testing.T) {
	t.Run("File changes captured", func(t *testing.T) {
		var counter uint64 = 0

		tmpfile, err := ioutil.TempFile("", "test")

		if err != nil {
			t.Fatal("Failed to create tmp file")
		}

		defer os.Remove(tmpfile.Name())

		go func() {
			watcherFunc := func() error {
				atomic.AddUint64(&counter, 1)
				return nil
			}

			WatchDirChanges(tmpfile.Name(), watcherFunc)
		}()

		writeToFile(tmpfile, "A")
		writeToFile(tmpfile, "B")

		if counter != 2 {
			t.Fatal("Failed to record file changes")
		}
	})

	t.Run("Watching ended upon error", func(t *testing.T) {
		var counter uint64 = 0

		tmpfile, err := ioutil.TempFile("", "test")

		if err != nil {
			t.Fatal("Failed to create tmp file")
		}

		defer os.Remove(tmpfile.Name())

		go func() {
			watcherFunc := func() error {
				if counter > 0 {
					return errors.New("Some error")
				}

				atomic.AddUint64(&counter, 1)

				return nil
			}

			WatchDirChanges(tmpfile.Name(), watcherFunc)
		}()

		// we do three writes here, but abort watching with an error after the first one
		writeToFile(tmpfile, "A")
		writeToFile(tmpfile, "B")
		writeToFile(tmpfile, "C")

		if counter != 1 {
			t.Fatal("Failed to record file changes")
		}
	})
}

func writeToFile(file *os.File, content string) {
	file.WriteString(content)
	time.Sleep(50 * time.Millisecond)
}
