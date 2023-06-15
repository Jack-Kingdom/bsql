package bsql

import (
	"context"
	"database/sql"
	"fmt"
	"go.uber.org/zap"
	"reflect"
	"strings"
	"time"
)

func QueryRows(ctx context.Context, rst interface{}, query string, args ...interface{}) error {
	if reflect.TypeOf(rst).Kind() != reflect.Pointer {
		return fmt.Errorf("QueryRows rst must be a pointer")
	}

	obj := reflect.Indirect(reflect.ValueOf(rst))

	objType := obj.Type()
	if objType.Kind() != reflect.Slice {
		return fmt.Errorf("QueryRows rst pointed must a slice")
	}

	var executed string
	start := time.Now()
	defer func() {
		zap.L().Info("QueryRows", zap.String("sql", executed), zap.Any("args", args), zap.Duration("duration", time.Since(start)))
	}()

	db, err := defaultPool.getMaster()
	if err != nil {
		return err
	}

	_, columns := getColumnsByType(objType.Elem(), opsTypeQuery)
	executed = strings.Replace(query, "*", columns, 1)
	stmt, err := db.PrepareContext(ctx, executed)
	if err != nil {
		return fmt.Errorf("err on prepare statement: %w", unifyError(defaultPool.driverName, err))
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return fmt.Errorf("err on stmt queryContext: %w", unifyError(defaultPool.driverName, err))
	}
	defer rows.Close()

	for rows.Next() {
		item := reflect.New(objType.Elem()).Elem()
		err = rows.Scan(getBinding(item.Addr().Interface(), opsTypeQuery)...)
		if err != nil {
			return fmt.Errorf("err on scan: %w", unifyError(defaultPool.driverName, err))
		}

		obj.Set(reflect.Append(obj, item))
	}
	return nil
}

func QueryRow(ctx context.Context, rst interface{}, query string, args ...interface{}) error {
	if reflect.TypeOf(rst).Kind() != reflect.Pointer {
		return fmt.Errorf("rst must be a pointer")
	}

	obj := reflect.Indirect(reflect.ValueOf(rst))

	objType := obj.Type()
	if objType.Kind() != reflect.Struct {
		return fmt.Errorf("rst pointed must a struct")
	}


	var executed string
	start := time.Now()
	defer func() {
		zap.L().Info("QueryRow", zap.String("sql", query), zap.Any("args", args), zap.Duration("duration", time.Since(start)))
	}()

	db, err := defaultPool.getMaster()
	if err != nil {
		return err
	}

	_, columns := getColumnsByType(objType, opsTypeQuery)
	executed = strings.Replace(query, "*", columns, 1)
	stmt, err := db.PrepareContext(ctx, executed)
	if err != nil {
		return fmt.Errorf("err on prepare statement: %w", unifyError(defaultPool.driverName, err))
	}
	defer stmt.Close()

	row := stmt.QueryRowContext(ctx, args...)
	err = row.Scan(getBinding(rst, opsTypeQuery)...)
	if err != nil {
		return fmt.Errorf("err on scan: %w", unifyError(defaultPool.driverName, err))
	}

	return nil
}

func Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	defer func() {
		zap.L().Info("Exec", zap.String("query", query), zap.Any("args", args), zap.Duration("duration", time.Since(start)))
	}()

	db, err := defaultPool.getMaster()
	if err != nil {
		return nil, err
	}

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("err on prepare statement: %w", unifyError(defaultPool.driverName, err))
	}
	defer stmt.Close()

	return stmt.ExecContext(ctx, args...)
}

func Insert(ctx context.Context, query string, data interface{}) (sql.Result, error) {
	var executed string
	start := time.Now()
	defer func() {
		zap.L().Info("Insert", zap.String("sql", executed), zap.Any("data", data), zap.Duration("duration", time.Since(start)))
	}()

	db, err := defaultPool.getMaster()
	if err != nil {
		return nil, err
	}

	n, columns := getColumns(data, opsTypeInsert)
	withColumn := strings.Replace(query, "*", columns, 1)
	executed = strings.Replace(withColumn, "*", genInPlaceHolder(n), 1)
	stmt, err := db.PrepareContext(ctx, executed)
	if err != nil {
		return nil, fmt.Errorf("err on prepare statement: %w", unifyError(defaultPool.driverName, err))
	}
	defer stmt.Close()

	return stmt.ExecContext(ctx, getBinding(data, opsTypeInsert)...)
}
