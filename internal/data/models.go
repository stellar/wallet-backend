package data

import (
	"errors"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

type Models struct {
	DB           db.ConnectionPool
	Account      *AccountModel
	IngestStore  *IngestStoreModel
	Payments     *PaymentModel
	Transactions *TransactionModel
}

func NewModels(db db.ConnectionPool, metricsService metrics.MetricsService) (*Models, error) {
	if db == nil {
		return nil, errors.New("ConnectionPool must be initialized")
	}

	return &Models{
		DB:           db,
		Account:      &AccountModel{DB: db, MetricsService: metricsService},
		IngestStore:  &IngestStoreModel{DB: db, MetricsService: metricsService},
		Payments:     &PaymentModel{DB: db, MetricsService: metricsService},
		Transactions: &TransactionModel{DB: db, MetricsService: metricsService},
	}, nil
}
