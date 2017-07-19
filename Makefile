BIN_NAME := binlog-parser
GOCC := env GOPATH=$(CURDIR)/_vendor:$(CURDIR) go
SRC_DIR := zalora/binlog-parser/...

TEST_DB_NAME := test_db
TEST_DB_SCHEMA_FILE := data/fixtures/test_db.sql

all:
	env CGO_ENABLED=0 $(GOCC) install -ldflags '-s' $(SRC_DIR)

deps:
	git submodule update --init

test: unit-test integration-test

unit-test:
	$(info ************ UNIT TESTS ************)
	env TZ="UTC" env DATA_DIR=$(CURDIR)/data $(GOCC) test -tags=unit -cover $(SRC_DIR)

integration-test: integration-test-setup
	$(info ************ INTEGRATION TESTS ************)
	env TEST_DB_DSN="root@/$(TEST_DB_NAME)" env TZ="UTC" env DATA_DIR=$(CURDIR)/data $(GOCC) test -tags=integration -cover $(SRC_DIR)

integration-test-setup:
	mysql -uroot -e 'DROP DATABASE IF EXISTS $(TEST_DB_NAME)'
	mysql -uroot < $(TEST_DB_SCHEMA_FILE)

integration-test-schema-dump:
	mysqldump --no-data -uroot -B $(TEST_DB_NAME) > $(TEST_DB_SCHEMA_FILE)

.PHONY: all deps test unit-test integration-test-setup integration-test integration-test-schema-dump
