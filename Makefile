GOCC := env GOPATH=$(CURDIR)/_vendor:$(CURDIR) go

all:
	$(GOCC) install zalora/binlog-parser

deps:
	git submodule --update --init

.PHONY: all
