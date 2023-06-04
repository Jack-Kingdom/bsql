package bsql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"reflect"
	"strings"
)

func getColumnName(field reflect.StructField) string {
	definedName := field.Tag.Get("bsql")

	if definedName == "-" {
		return ""
	}

	if definedName != "" {
		return definedName
	}

	return field.Name
}

func getColumns(obj interface{}) string {
	var val reflect.Value
	if reflect.TypeOf(obj).Kind() == reflect.Ptr {
		val = reflect.ValueOf(obj).Elem()
	} else {
		val = reflect.ValueOf(obj)
	}

	valType := val.Type()
	if valType.Kind() != reflect.Struct {
		panic(errors.New("obj not a struct"))
	}

	var names []string
	for i := 0; i < val.NumField(); i++ {
		column := getColumnName(val.Type().Field(i))
		if column == "" {
			continue
		}
		names = append(names, column)
	}
	return strings.Join(names, ", ")
}

func getColumnsByType(typ reflect.Type) string {
	var names []string
	for i := 0; i < typ.NumField(); i++ {
		column := getColumnName(typ.Field(i))
		if column == "" {
			continue
		}
		names = append(names, column)
	}
	return strings.Join(names, ", ")
}

func getBinding(obj interface{}) []interface{} {
	val := reflect.ValueOf(obj)
	var addrs []interface{}
	for i := 0; i < val.Elem().NumField(); i++ {
		column := getColumnName(val.Elem().Type().Field(i))
		if column == "" {
			continue
		}

		addr := val.Elem().Field(i).Addr().Interface()
		addrs = append(addrs, addr)
	}
	return addrs
}

var (
	ErrTableNotExists = errors.New("table not exists")
	ErrNoRecord       = errors.New("no record")
	ErrUndefined      = errors.New("undefined error")
)

func unifyError(driverName string, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNoRecord
	}

	switch driverName {
	case "mysql":
		mysqlErr, ok := err.(*mysql.MySQLError)
		if !ok {
			panic("mysql error type not match")
		}

		switch mysqlErr.Number {
		case 1146:
			return ErrTableNotExists
		default:
			return fmt.Errorf("undefined err: %w, content: %s", ErrUndefined, mysqlErr.Error())
		}
	}
	return err
}
