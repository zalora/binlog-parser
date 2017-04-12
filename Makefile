GOCC := env GOPATH=$(CURDIR) go

all:
	$(GOCC) install zalora.com/auditor/parse-binlog/

.PHONY: all
