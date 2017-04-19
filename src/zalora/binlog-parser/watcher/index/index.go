package index

import (
	"bufio"
	"fmt"
	"github.com/golang/glog"
	"io/ioutil"
	"os"
	"strings"
)

type Index struct {
	lines    []string
	filename string
}

func NewIndex(file *os.File) Index {
	scanner := bufio.NewScanner(file)
	var lines []string

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	return Index{lines: lines, filename: file.Name()}
}

func (i *Index) Append(line string) {
	i.lines = append(i.lines, line)
}

func (i *Index) Sync() error {
	return i.SyncFile(i.filename)
}

func (i *Index) SyncFile(filename string) error {
	data := strings.Join(i.lines, "\n")
	return ioutil.WriteFile(filename, []byte(fmt.Sprintf("%s\n", data)), 0644)
}

func (i *Index) Diff(other Index) []string {
	var diff []string

	glog.Infof("Starting diff: %v vs %v", i.lines, other.lines)

	for _, line := range i.lines {
		found := false
		for _, other_line := range other.lines {
			if other_line == line {
				found = true
				break
			}
		}

		if !found {
			diff = append(diff, line)
		}
	}

	return diff
}
