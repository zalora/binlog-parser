package parser

import (
	"encoding/json"
	"os"
	"zalora/binlog-parser/parser/messages"
	"github.com/golang/glog"
)

func DumpMessageAsJsonToFile(f *os.File) func(messages.Message) error {
	return func(message messages.Message) error {
		json, err := json.MarshalIndent(message, "", "    ")

		if err != nil {
			glog.Errorf("Failed to convert message to JSON: %s", err)
			return err
		}

		n, err := f.Write(json)

		if err != nil {
			glog.Errorf("Failed to write message JSON to file %s", err)
			return err
		}

		glog.Infof("Wrote %d bytes to file", n)

		return nil
	}
}
