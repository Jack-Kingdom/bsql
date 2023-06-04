package bsql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"reflect"
	"strings"
)

/*
 * get column name from struct tag
 * use tag column first, then json, then yaml
 */
func getColumnName(tag reflect.StructTag) string {
	column := tag.Get("column")

	if column == "-" {
		return ""
	}
	if column != "" {
		return column
	}

	column = tag.Get("json")
	if column != "" && column != "-" {
		return column
	}

	column = tag.Get("yaml")
	if column != "" && column != "-" {
		return column
	}

	return ""
}

/*
 * get scoped column name from struct tag
 */
func getScopedName(tag reflect.StructTag) string {
	column := getColumnName(tag)
	if column == "" {
		return ""
	}

	scope := tag.Get("scope")
	if scope != "" {
		return fmt.Sprintf("%s.%s", scope, column)
	}
	return column
}

func getColumns(obj interface{}) string {
	var val reflect.Value
	if reflect.TypeOf(obj).Kind() == reflect.Ptr {
		val = reflect.ValueOf(obj).Elem()
	} else {
		val = reflect.ValueOf(obj)
	}

	var names []string
	for i := 0; i < val.NumField(); i++ {
		column := getScopedName(val.Type().Field(i).Tag)
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
		column := getScopedName(typ.Field(i).Tag)
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
		column := getColumnName(val.Elem().Type().Field(i).Tag)
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
