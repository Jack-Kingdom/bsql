package bsql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/Jack-Kingdom/bsql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"os"
	"testing"
	"time"
)

func initEnvironment() error {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)

	err := godotenv.Load()
	if err != nil {
		return err
	}

	db, err := sql.Open("mysql", os.Getenv("MYSQL_CONN_STR"))
	if err != nil {
		return fmt.Errorf("err on open db connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("err on ping db: %w", err)
	}

	return bsql.RegisterMaster("mysql", db)
}

var (
	createTableSql = `
CREATE TABLE IF NOT EXISTS user (
	id INTEGER PRIMARY KEY AUTO_INCREMENT,
	username VARCHAR(255) NOT NULL,
	password VARCHAR(255) NOT NULL,
	created_at DATETIME NOT NULL
)`
)

type UserType struct {
	Id        int64       `json:"id" bsql:"id,insert:ignore"`
	Username  string    `json:"username"`
	Password  string    `json:"password"`
	CreatedAt time.Time `json:"created_at" bsql:"created_at"`
}

func TestCURD(t *testing.T) {
	err := initEnvironment()
	require.Nil(t, err)

	defer func() {
		_, err = bsql.Exec(context.TODO(), "DROP TABLE IF EXISTS user")
		require.Nil(t, err)
	}()

	var users []UserType
	err = bsql.QueryRows(context.TODO(), &users, "SELECT * FROM user LIMIT 10")
	assert.True(t, errors.Is(err, bsql.ErrTableNotExists))

	_, err = bsql.Exec(context.TODO(), createTableSql)
	require.Nil(t, err)

	var user UserType
	err = bsql.QueryRow(context.TODO(), &user, "SELECT * FROM user LIMIT 10")
	assert.True(t, errors.Is(err, bsql.ErrNoRecord))

	newUser := UserType{
		Username:  "jack",
		Password:  "123",
		CreatedAt: time.Now().UTC(),
	}
	var sqlResult sql.Result
	sqlResult, err = bsql.Insert(context.TODO(), "INSERT INTO user (*) VALUES (*)", &newUser)
	require.Nil(t, err)
	newUser.Id, err = sqlResult.LastInsertId()
	require.Nil(t, err)

	err = bsql.QueryRows(context.TODO(), &users, "SELECT * FROM user LIMIT 10")
	require.Nil(t, err)
	assert.Equal(t, 1, len(users))

	insertedUser := users[0]
	assert.Equal(t, newUser.Id, insertedUser.Id)
	assert.Equal(t, newUser.Username, insertedUser.Username)
	assert.Equal(t, newUser.Password, insertedUser.Password)
	assert.Less(t, newUser.CreatedAt.Unix()-5, insertedUser.CreatedAt.Unix())
	assert.Greater(t, newUser.CreatedAt.Unix()+5, insertedUser.CreatedAt.Unix())

	_, err = bsql.Exec(context.TODO(), "UPDATE user SET username = ? WHERE id = ?", "jack-kingdom", insertedUser.Id)
	require.Nil(t, err)

	users = users[:0]
	err = bsql.QueryRows(context.TODO(), &users, "SELECT * FROM user LIMIT 10")
	require.Nil(t, err)
	assert.Equal(t, 1, len(users))

	updatedUser := users[0]
	assert.Equal(t, updatedUser.Username, "jack-kingdom")

	var singleUser UserType
	err = bsql.QueryRow(context.TODO(), &singleUser, "SELECT * FROM user WHERE id = ?", updatedUser.Id)
	require.Nil(t, err)
	assert.Equal(t, updatedUser.Username, singleUser.Username)
	assert.Equal(t, updatedUser.Password, singleUser.Password)
	assert.Equal(t, updatedUser.CreatedAt, singleUser.CreatedAt)
}
