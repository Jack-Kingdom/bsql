package bsql

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestGetColumnName(t *testing.T) {
	input := `json:"word"`
	column := getColumnName(reflect.StructTag(input))
	assert.Equal(t, "word", column)

	input = `json:"word" column:"hello"`
	column = getColumnName(reflect.StructTag(input))
	assert.Equal(t, "hello", column)

	input = `json:"word" column:"-"`
	column = getColumnName(reflect.StructTag(input))
	assert.Equal(t, "", column)
}

func TestGetScopedName(t *testing.T) {
	input := `json:"word"`
	column := getScopedName(reflect.StructTag(input))
	assert.Equal(t, "word", column)

	input = `json:"word" column:"hello"`
	column = getScopedName(reflect.StructTag(input))
	assert.Equal(t, "hello", column)

	input = `json:"word" column:"-"`
	column = getScopedName(reflect.StructTag(input))
	assert.Equal(t, "", column)

	input = `json:"word" column:"hello" scope:"t"`
	column = getScopedName(reflect.StructTag(input))
	assert.Equal(t, "t.hello", column)
}

func TestGetColumns(t *testing.T) {
	type Test struct {
		Hello string `json:"hello"`
		Word  string `json:"word" scope:"t"`
	}
	test := Test{}

	columns := getColumns(test)
	assert.Equal(t, "hello, t.word", columns)

	columns = getColumns(&test)
	assert.Equal(t, "hello, t.word", columns)

	columns = getColumnsByType(reflect.TypeOf(test))
	assert.Equal(t, "hello, t.word", columns)
}
