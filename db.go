package bsql

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
)

var (
	defaultPool             dbPoolType
	ErrNoDatabaseRegistered = errors.New("no db registered")
	ErrDriverNameNotMatch   = errors.New("driver name not match")
)

type dbPoolType struct {
	driverName string
	master     []*sql.DB
}

func (dbPool *dbPoolType) registerMaster(driverName string, db *sql.DB) error {
	if dbPool.driverName == "" {
		dbPool.driverName = driverName
	} else if dbPool.driverName != driverName {
		return fmt.Errorf("%w: %s, %s", ErrDriverNameNotMatch, dbPool.driverName, driverName)
	}

	dbPool.master = append(dbPool.master, db)
	return nil
}

func RegisterMaster(driverName string, db *sql.DB) error {
	return defaultPool.registerMaster(driverName, db)
}

func (dbPool *dbPoolType) getMaster() (*sql.DB, error) {
	if len(dbPool.master) == 0 {
		return nil, ErrNoDatabaseRegistered
	}
	return dbPool.master[rand.Intn(len(dbPool.master))], nil
}

func GetMaster() (*sql.DB, error) {
	return defaultPool.getMaster()
}
