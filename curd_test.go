package bsql_test

import (
	"context"
	"errors"
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

	return bsql.RegisterMaster("mysql", os.Getenv("MYSQL_CONN_STR"))
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
	Id        int       `json:"id"`
	Username  string    `json:"username"`
	Password  string    `json:"password"`
	CreatedAt time.Time `json:"created_at"`
}

func TestCURD(t *testing.T) {
	t.Parallel()
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

	_, err = bsql.Exec(context.TODO(), "INSERT INTO user (username, password, created_at) VALUES (?, ?, ?)", "jack", "123456", time.Now().UTC())
	require.Nil(t, err)

	err = bsql.QueryRows(context.TODO(), &users, "SELECT * FROM user LIMIT 10")
	require.Nil(t, err)
	assert.Equal(t, 1, len(users))

	insertedUser := users[0]
	assert.Equal(t, insertedUser.Username, "jack")
	assert.Equal(t, insertedUser.Password, "123456")
	assert.Less(t, time.Time{}, insertedUser.CreatedAt)

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
