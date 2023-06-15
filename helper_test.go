package bsql

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestGetColumns(t *testing.T) {
	type Test struct {
		Id    int    `json:"id" bsql:",insert:ignore"`
		Hello string `json:"hello"`
		World string `json:"word" bsql:"bingo"`
	}
	test := Test{}

	_, columns := getColumns(test, opsTypeQuery)
	assert.Equal(t, "id, hello, bingo", columns)

	_, columns = getColumns(&test, opsTypeQuery)
	assert.Equal(t, "id, hello, bingo", columns)

	_, columns = getColumnsByType(reflect.TypeOf(test), opsTypeQuery)
	assert.Equal(t, "id, hello, bingo", columns)

	_, columns = getColumns(&test, opsTypeInsert)
	assert.Equal(t, "hello, bingo", columns)
}
