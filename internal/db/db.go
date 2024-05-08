package db

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stellar/go/support/errors"
)

type ConnectionPool interface {
	Close() error
	Ping() error
	DriverName() string
	sqlx.ExecerContext
	sqlx.QueryerContext
	sqlx.PreparerContext
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

// Make sure *DBConnectionPoolImplementation implements DBConnectionPool:
var _ ConnectionPool = (*DBConnectionPoolImplementation)(nil)

type DBConnectionPoolImplementation struct {
	*sqlx.DB
	dataSourceName string
}

func (db *DBConnectionPoolImplementation) DSN(ctx context.Context) (string, error) {
	return db.dataSourceName, nil
}

const (
	MaxDBConnIdleTime = 10 * time.Second
	MaxOpenDBConns    = 30
)

func OpenDBConnectionPool(dataSourceName string) (ConnectionPool, error) {
	sqlxDB, err := sqlx.Open("postgres", dataSourceName)
	if err != nil {
		return nil, errors.Wrap(err, "error creating app DB connection pool")
	}
	sqlxDB.SetConnMaxIdleTime(MaxDBConnIdleTime)
	sqlxDB.SetMaxOpenConns(MaxOpenDBConns)

	err = sqlxDB.Ping()
	if err != nil {
		return nil, errors.Wrap(err, "error pinging app DB connection pool")
	}

	return &DBConnectionPoolImplementation{DB: sqlxDB}, nil
}
