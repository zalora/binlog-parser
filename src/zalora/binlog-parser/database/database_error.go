package database

import (
	"fmt"
	"reflect"
)

type ConnectionError struct {
	errorMessage string
}

func newConnectionError(err error) ConnectionError {
	return ConnectionError{fmt.Sprintf("Connection error %s %s", reflect.TypeOf(err), err)}
}

func (e *ConnectionError) Error() string {
	return e.errorMessage
}

type QueryError struct {
	errorMessage string
}

func newQueryError(err error) QueryError {
	return QueryError{fmt.Sprintf("Query error %s %s", reflect.TypeOf(err), err)}
}

func (e *QueryError) Error() string {
	return e.errorMessage
}
