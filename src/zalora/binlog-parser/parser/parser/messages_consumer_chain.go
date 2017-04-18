package parser

import (
	"encoding/json"
	"github.com/golang/glog"
	"os"
	"zalora/binlog-parser/parser/messages"
)

type MessagesConsumerChain struct {
	predicates []predicate
	collectors []collector
}

type predicate func(message messages.Message) bool

type collector func(message messages.Message) error

func NewMessagesConsumerChain() MessagesConsumerChain {
	return MessagesConsumerChain{}
}

func (c *MessagesConsumerChain) IncludeTables(tables ...string) {
	c.predicates = append(c.predicates, tablesPredicate(tables...))
}

func (c *MessagesConsumerChain) IncludeSchemas(schemas ...string) {
	c.predicates = append(c.predicates, schemaPredicate(schemas...))
}

func (c *MessagesConsumerChain) CollectAsJsonInFile(f *os.File) {
	c.collectors = append(c.collectors, jsonFileCollector(f))
}

func (c *MessagesConsumerChain) consumeMessage(message messages.Message) error {
	for _,predicate := range c.predicates {
		pass := predicate(message)

		if !pass {
			return nil
		}
	}

	for _,collector := range c.collectors {
		collector_err := collector(message)

		if collector_err != nil {
			return collector_err
		}
	}

	return nil
}

func schemaPredicate(databases ...string) predicate {
	return func(message messages.Message) bool {
		if message.GetHeader().Schema == "" {
			return true
		}

		return contains(databases, message.GetHeader().Schema)
	}
}

func tablesPredicate(tables ...string) predicate {
	return func(message messages.Message) bool {
		if message.GetHeader().Table == "" {
			return true
		}

		return contains(tables, message.GetHeader().Table)
	}
}

func jsonFileCollector(f *os.File) collector {
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

func contains(s []string, e string) bool {
    for _, a := range s {
        if a == e {
            return true
        }
    }

    return false
}
