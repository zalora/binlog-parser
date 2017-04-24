BIN_NAME := binlog-parser
GOCC := env GOPATH=$(CURDIR)/_vendor:$(CURDIR) go
SRC_DIR := zalora/binlog-parser/...
TEST_DB_SCHEMA_FILE := data/fixtures/test_db.sql

all:
	$(GOCC) install $(SRC_DIR)

deps:
	git submodule update --init

test: unit-test integration-test

unit-test:
	$(info ************ UNIT TESTS ************)
	env TZ="UTC" env DATA_DIR=$(CURDIR)/data $(GOCC) test -tags=unit -cover $(SRC_DIR)

integration-test: all integration-test-setup
	$(info ************ INTEGRATION TESTS ************)
	env TZ="UTC" env DATA_DIR=$(CURDIR)/data $(GOCC) test -tags=integration -cover $(SRC_DIR)

integration-test-setup:
	mysql -uroot -e 'DROP DATABASE IF EXISTS test_db'
	mysql -uroot < $(TEST_DB_SCHEMA_FILE)

integration-test-schema-dump:
	mysqldump --no-data -uroot -B test_db > $(TEST_DB_SCHEMA_FILE)

.PHONY: all deps test unit-test integration-test-setup integration-test integration-test-schema-dump
