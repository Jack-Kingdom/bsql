package bsql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"reflect"
	"strings"
)

type opsType int

const (
	opsTypeQuery opsType = iota
	opsTypeInsert
)

func getColumnName(field reflect.StructField, ops opsType) string {
	tag := field.Tag.Get("bsql")

	configuration := strings.Split(tag, ",")

	var fieldName string
	for i, config := range configuration {
		if i == 0 {
			if config == "-" {
				return ""
			}

			if config == "" {
				fieldName = field.Name
			} else {
				fieldName = config
			}
		} else {
			if config == "insert:ignore" && ops == opsTypeInsert {
				return ""
			}
		}
	}

	return strings.ToLower(fieldName)
}

func getColumns(obj interface{}, ops opsType) (int, string) {
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
		column := getColumnName(val.Type().Field(i), ops)
		if column == "" {
			continue
		}
		names = append(names, column)
	}
	return len(names), strings.Join(names, ", ")
}

func getColumnsByType(typ reflect.Type, ops opsType) (int, string) {
	var names []string
	for i := 0; i < typ.NumField(); i++ {
		column := getColumnName(typ.Field(i), ops)
		if column == "" {
			continue
		}
		names = append(names, column)
	}
	return len(names), strings.Join(names, ", ")
}

func getBinding(obj interface{}, ops opsType) []interface{} {
	val := reflect.ValueOf(obj)
	var addrs []interface{}
	for i := 0; i < val.Elem().NumField(); i++ {
		column := getColumnName(val.Elem().Type().Field(i), ops)
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
			return fmt.Errorf("unknown error: %w", err)
		}

		switch mysqlErr.Number {
		case 1146:
			return ErrTableNotExists
		default:
			return fmt.Errorf("undefined mysql err: %w, content: %s", ErrUndefined, mysqlErr.Error())
		}
	}
	return err
}

func genInPlaceHolder(num int) string {
	var sb strings.Builder
	for i := 0; i < num; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("?")
	}
	return sb.String()
}