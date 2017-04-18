GOCC := env GOPATH=$(CURDIR)/_vendor:$(CURDIR) go
SRC_DIR := zalora/binlog-parser/...

all:
	$(GOCC) install $(SRC_DIR)

deps:
	git submodule update --init

test-setup:
	mysql -uroot < data/fixtures/test_db.sql

test:
	env DATA_DIR=$(CURDIR)/data $(GOCC) test -cover $(SRC_DIR)

.PHONY: all deps test test-setup
