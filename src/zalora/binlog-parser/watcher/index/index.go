package index

import (
	"os"
	"bufio"
	"strings"
	"io/ioutil"
	"fmt"
)

type Index struct {
	lines []string
	filename string
}

func NewIndex(filename string) (Index, error) {
	file, err := os.Open(filename)

	if err != nil {
		return Index{}, err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []string

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	return Index{lines: lines, filename: filename}, nil
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

func difference(slice1 []string, slice2 []string) []string {
    var diff []string

    // Loop two times, first to find slice1 strings not in slice2,
    // second loop to find slice2 strings not in slice1
    for i := 0; i < 2; i++ {
        for _, s1 := range slice1 {
            found := false
            for _, s2 := range slice2 {
                if s1 == s2 {
                    found = true
                    break
                }
            }
            // String not found. We add it to return slice
            if !found {
                diff = append(diff, s1)
            }
        }
        // Swap the slices, only if it was the first loop
        if i == 0 {
            slice1, slice2 = slice2, slice1
        }
    }

    return diff
}
