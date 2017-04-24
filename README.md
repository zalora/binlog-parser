# binlog-parser

A tool for parsing a MySQL binlog file to JSON. Reads a binlog input file, queries a database for field names, writes JSON to stdout. The output looks like this:

    {
        "Header": {
            "Schema": "test_db",
            "Table": "buildings",
            "BinlogMessageTime": "2017-04-13T06:34:30Z",
            "BinlogPosition": 397,
            "XId": 9
        },
        "Type": "Insert",
        "Data": {
            "Row": {
                "address": "3950 North 1st Street CA 95134",
                "building_name": "ACME Headquaters",
                "building_no": 1
            },
            "MappingNotice": ""
        }
    }
    ...

# Installation

Requires Go version 1.7 or higher.

    $ git clone https://github.com/zalora/binlog-parser.git
    $ cd binlog-parser
    $ git submodule update --init
    $ make
    $ ./bin/binlog-parser -h

## Assumptions

- It is assumed that MySQL row-based binlog format is used (or mixed, but be aware, that then only the row-formatted data in mixed binlogs can be extracted)
- This tool is written with MySQL 5.6 in mind, although it should also work for MariaDB when GTIDs are not used

# Usage

Run `binlog-parser -h` to get the list of available options:

    Usage:	binlog-parser binlog [options ...]

    Options are:

      -alsologtostderr
        	log to standard error as well as files
      -include_schemas string
        	comma-separated list of schemas to include
      -include_tables string
        	comma-separated list of tables to include
      -log_backtrace_at value
        	when logging hits line file:N, emit a stack trace
      -log_dir string
        	If non-empty, write log files in this directory
      -logtostderr
        	log to standard error instead of files
      -prettyprint
        	Pretty print json
      -stderrthreshold value
        	logs at or above this threshold go to stderr
      -v value
        	log level for V logs
      -vmodule value
        	comma-separated list of pattern=N settings for file-filtered logging

    Required environment variables:

    DB_DSN	 Database connection string, needs read access to information_schema

## Matching field names and data

The mysql binlog format doesn't include the fieldnames for row events (INSERT/UPDATE/DELETE). As the goal of the parser is to output
usable JSON, it connects to a running MySQL instance and queries the `information_schema` database for information on field names in the table.

The database connection is creatd by using the environment variable `DB_DSN`, which should contain the database credentials in the form of
`user:password@/dbname` - the format that the [Go MySQL driver](https://godoc.org/github.com/go-sql-driver/mysql) uses.

## Effect of schema changes

As this tool doesn't keep an internal representation of the database schema, it is very well possible that the database schema and the schema used in the
queries in the binlog file already have diverged (e. g. parsing a binlog file from a few days ago, but the schema on the main database already changed
by dropping or adding columns).

The parser will NOT make an attempt to map data to fields in a table if the information schema retuns more or too less columns
compared to the format found in the binlog. The field names will be mapped as "unknown":

    {
        "Header": {
            "Schema": "test_db",
            "Table": "employees",
            "BinlogMessageTime": "2017-04-13T08:02:04Z",
            "BinlogPosition": 635,
            "XId": 8
        },
        "Type": "Insert",
        "Data": {
            "Row": {
                "(unknown_0)": 1,
                "(unknown_1)": "2017-04-13",
                "(unknown_2)": "Max",
                "(unknown_3)": "Mustermann"
            },
            "MappingNotice": "column names array is missing field(s), will map them as unknown_*"
        }
    }

### More complex case

Changing the order of fields in a table can lead to unexpected parser results. Consider an example binlog file `A.bin`.
A query like `INSERT INTO db.foo SET field_1 = 10, field_2 = 20` will look in the binlog like this:

    ...
    ### INSERT INTO `db`.`foo`
    ### SET
    ###   @1=20 /* ... */
    ###   @2=20 /* ... */
    ...

The parser queries `information_schema` for the field names of the `db.foo` table:

    +-------------+-----+
    | Field       | ... |
    +-------------+-----+
    | field_1     | ... |
    | field_2     | ... |
    +-------------+-----+

The fields will be mapped by the parser in the order as specified in the table and the JSON will look like this:

    {
        ...
        "Type": "Insert",
        "Data": {
            "Row": {
                "field_1": 10,
                "field_2": 20
            }
        }
    }
    ...

Now if a schema change happened after some time, `db.foo` fields might look now like this (the order of the fiels changed):

    +-------------+-----+
    | Field       | ... |
    +-------------+-----+
    | field_2     | ... |
    | field_1     | ... |
    +-------------+-----+

If you parse the same binlog file `A.bin` now again, but against the new schema of `db.foo` (in which the fields changed position), the resulting JSON
will look like that:


    {
        ...
        "Type": "Insert",
        "Data": {
            "Row": {
                "field_2": 10,
                "field_1": 20
            }
        }
    }
    ...

This means you have to be very careful when parsing old binlog files, as the db schema can have evolved since the binlog was generated and the parser
has no way of knowing of these changes.

If this limitation is not acceptable, some tools like [Maxwell's Daemon by Zendesk](https://github.com/zendesk/maxwell) can work around that issue at the cost of greater complexity.
