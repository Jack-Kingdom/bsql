package bsql

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestGetColumns(t *testing.T) {
	type Test struct {
		Hello string `json:"hello" bsql:"hello"`
		Word  string `json:"word"`
	}
	test := Test{}

	columns := getColumns(test)
	assert.Equal(t, "hello, Word", columns)

	columns = getColumns(&test)
	assert.Equal(t, "hello, Word", columns)

	columns = getColumnsByType(reflect.TypeOf(test))
	assert.Equal(t, "hello, Word", columns)
}
