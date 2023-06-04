package bsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"	// TODO: remove this
	"go.uber.org/zap"
	"reflect"
	"strings"
	"time"
)

var (
	ErrNoRecord       = errors.New("no record")
	ErrTableNotExists = errors.New("table not exists")
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

	start := time.Now()
	defer func() {
		zap.L().Info("QueryRows end", zap.String("sql", query), zap.Any("args", args), zap.Duration("duration", time.Since(start)))
	}()
	zap.L().Info("QueryRows start", zap.String("sql", query), zap.Any("args", args))

	db, err := pool.getMaster()
	if err != nil {
		return err
	}

	formattedSql := strings.Replace(query, "*", getColumnsByType(objType.Elem()), 1)
	zap.L().Debug("QueryRows formattedSql", zap.String("sql", formattedSql))
	stmt, err := db.PrepareContext(ctx, formattedSql)
	if err != nil {
		if mysqlErr, ok := err.(*mysql.MySQLError); ok {
			if mysqlErr.Number == 1146 {
				return ErrTableNotExists
			}
		}
		return fmt.Errorf("err on prepare statement: %s", err)
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return fmt.Errorf("err on stmt queryContext: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		item := reflect.New(objType.Elem()).Elem()
		err = rows.Scan(getBinding(item.Addr().Interface())...)
		if err != nil {
			return fmt.Errorf("err on scan: %s", err)
		}

		obj.Set(reflect.Append(obj, item))
	}

	if obj.Len() == 0 {
		return ErrNoRecord
	}
	return nil
}

func QueryRow(ctx context.Context, rst interface{}, query string, args ...interface{}) error {
	start := time.Now()
	defer func() {
		zap.L().Info("QueryRow end", zap.String("sql", query), zap.Any("args", args), zap.Duration("duration", time.Since(start)))
	}()
	zap.L().Info("QueryRow start", zap.String("sql", query), zap.Any("args", args))

	db, err := pool.getMaster()
	if err != nil {
		return err
	}

	if reflect.TypeOf(rst).Kind() != reflect.Pointer {
		return fmt.Errorf("rst must be a pointer")
	}

	obj := reflect.Indirect(reflect.ValueOf(rst))

	objType := obj.Type()
	if objType.Kind() != reflect.Struct {
		return fmt.Errorf("rst pointed must a struct")
	}

	formattedSql := strings.Replace(query, "*", getColumnsByType(objType), 1)
	zap.L().Debug("QueryRow formattedSql", zap.String("sql", formattedSql))
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		if mysqlErr, ok := err.(*mysql.MySQLError); ok {
			if mysqlErr.Number == 1146 {
				return ErrTableNotExists
			}
		}
		return fmt.Errorf("err on prepare statement: %s", err)
	}
	defer stmt.Close()

	row := stmt.QueryRowContext(ctx, args...)
	err = row.Scan(getBinding(rst)...)
	if err != nil {
		return fmt.Errorf("err on scan: %s", err)
	}

	return nil
}


func Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	defer func() {
		zap.L().Info("Exec end", zap.String("query", query), zap.Any("args", args), zap.Duration("duration", time.Since(start)))
	}()
	zap.L().Info("Exec start", zap.String("query", query), zap.Any("args", args))

	db, err := pool.getMaster()
	if err != nil {
		return nil, err
	}

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("err on prepare statement: %s", err)
	}
	defer stmt.Close()

	return stmt.ExecContext(ctx, args...)
}