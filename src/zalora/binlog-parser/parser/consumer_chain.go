package parser

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"io"
	"zalora/binlog-parser/parser/messages"
)

type ConsumerChain struct {
	predicates  []predicate
	collectors  []collector
	prettyPrint bool
}

type predicate func(message messages.Message) bool

type collector func(message messages.Message) error

func NewConsumerChain() ConsumerChain {
	return ConsumerChain{}
}

func (c *ConsumerChain) IncludeTables(tables ...string) {
	c.predicates = append(c.predicates, tablesPredicate(tables...))
}

func (c *ConsumerChain) IncludeSchemas(schemas ...string) {
	c.predicates = append(c.predicates, schemaPredicate(schemas...))
}

func (c *ConsumerChain) PrettyPrint(prettyPrint bool) {
	c.prettyPrint = prettyPrint
}

func (c *ConsumerChain) CollectAsJson(stream io.Writer, prettyPrint bool) {
	c.collectors = append(c.collectors, streamCollector(stream, prettyPrint))
}

func (c *ConsumerChain) consumeMessage(message messages.Message) error {
	for _, predicate := range c.predicates {
		pass := predicate(message)

		if !pass {
			return nil
		}
	}

	for _, collector := range c.collectors {
		collector_err := collector(message)

		if collector_err != nil {
			return collector_err
		}
	}

	return nil
}

func streamCollector(stream io.Writer, prettyPrint bool) collector {
	return func(message messages.Message) error {
		json, err := marshalMessage(message, prettyPrint)

		if err != nil {
			glog.Errorf("Failed to convert message to JSON: %s", err)
			return err
		}

		n, err := stream.Write([]byte(fmt.Sprintf("%s\n", json)))

		if err != nil {
			glog.Errorf("Failed to write message JSON to file %s", err)
			return err
		}

		glog.V(1).Infof("Wrote %d bytes to stream", n)

		return nil
	}
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

func marshalMessage(message messages.Message, prettyPrint bool) ([]byte, error) {
	if prettyPrint {
		return json.MarshalIndent(message, "", "    ")
	}

	return json.Marshal(message)
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}
