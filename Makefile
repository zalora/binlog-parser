GOCC := env GOPATH=$(CURDIR) go

all:
	$(GOCC) install zalora/binlog-parser

.PHONY: all
