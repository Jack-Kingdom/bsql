package bsql

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
)

var (
	pool dbPoolType
	ErrNoDatabaseRegistered = errors.New("no db registered")
)

type dbPoolType struct {
	master []*sql.DB
}

func (dbPool *dbPoolType) registerMaster(driverName, connStr string) error {
	db, err := sql.Open(driverName, connStr)
	if err != nil {
		return fmt.Errorf("err on open db connection: %w", err)
	}

	err = db.Ping()
	if err !=nil {
		return fmt.Errorf("err on ping db: %w, %s", err, connStr)
	}

	dbPool.master = append(dbPool.master, db)
	return nil
}

func RegisterMaster(driverName, connStr string) error {
	return pool.registerMaster(driverName, connStr)
}

func (dbPool *dbPoolType) getMaster() (*sql.DB, error) {
	if len(dbPool.master) == 0 {
		return nil, ErrNoDatabaseRegistered
	}
	return dbPool.master[rand.Intn(len(dbPool.master))], nil
}

func GetMaster() (*sql.DB, error) {
	return pool.getMaster()
}