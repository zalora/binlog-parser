BIN_NAME := binlog-parser
GOCC := env GOPATH=$(CURDIR)/_vendor:$(CURDIR) go
SRC_DIR := zalora/binlog-parser/...

all:
	$(GOCC) install $(SRC_DIR)

deps:
	git submodule update --init

test: unit-test integration-test

unit-test:
	$(info ************ UNIT TESTS ************)
	env DATA_DIR=$(CURDIR)/data $(GOCC) test -tags=unit -cover $(SRC_DIR)

integration-test: all
	$(info ************ INTEGRATION TESTS ************)
	env DATA_DIR=$(CURDIR)/data $(GOCC) test -tags=integration -cover $(SRC_DIR)

integration-test-setup:
	mysql -uroot < data/fixtures/test_db.sql

.PHONY: all deps test unit-test integration-test-setup integration-test
