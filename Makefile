GOCC := env GOPATH=$(CURDIR)/_vendor:$(CURDIR) go

all:
	$(GOCC) install zalora/binlog-parser

.PHONY: all
