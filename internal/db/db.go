package db

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stellar/go/support/errors"
)

type DBConnectionPool interface {
	Close() error
	Ping() error
	DriverName() string
	sqlx.ExecerContext
	sqlx.QueryerContext
	sqlx.PreparerContext
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

type DBConnectionPoolImplementation struct {
	*sqlx.DB
}

// Make sure *DBConnectionPoolImplementation implements DBConnectionPool:
var _ DBConnectionPool = (*DBConnectionPoolImplementation)(nil)

const (
	MaxDBConnIdleTime = 10 * time.Second
	MaxOpenDBConns    = 30
)

func OpenDBConnectionPool(dataSourceName string) (DBConnectionPool, error) {
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
