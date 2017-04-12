GOCC := env GOPATH=$(CURDIR) go

all:
	$(GOCC) install zalora/binlog-parser

deps:
	$(GOCC) get github.com/go-sql-driver/mysql
	$(GOCC) get github.com/siddontang/go-mysql/...

.PHONY: all deps
