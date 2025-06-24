package data

import (
	"errors"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

type Models struct {
	Payments *PaymentModel
	Account  *AccountModel
	Contract *ContractModel
}

func NewModels(db db.ConnectionPool, metricsService metrics.MetricsService) (*Models, error) {
	if db == nil {
		return nil, errors.New("ConnectionPool must be initialized")
	}

	return &Models{
		Payments: &PaymentModel{DB: db, MetricsService: metricsService},
		Account:  &AccountModel{DB: db, MetricsService: metricsService},
		Contract: &ContractModel{DB: db, MetricsService: metricsService},
	}, nil
}
