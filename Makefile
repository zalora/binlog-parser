GOCC := env GOPATH=$(CURDIR)/_vendor:$(CURDIR) go
SRC_DIR := zalora/binlog-parser/...

all:
	$(GOCC) install $(SRC_DIR)

deps:
	git submodule --update --init

test:
	env DATA_DIR=$(CURDIR)/data $(GOCC) test -v $(SRC_DIR)

.PHONY: all deps test
