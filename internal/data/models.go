package data

import (
	"errors"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

type Models struct {
	DB                    db.ConnectionPool
	Account               *AccountModel
	Contract              ContractModelInterface
	TrustlineAsset        TrustlineAssetModelInterface
	TrustlineBalance      TrustlineBalanceModelInterface
	AccountContractTokens AccountContractTokensModelInterface
	IngestStore           *IngestStoreModel
	Operations            *OperationModel
	Transactions          *TransactionModel
	StateChanges          *StateChangeModel
}

func NewModels(db db.ConnectionPool, metricsService metrics.MetricsService) (*Models, error) {
	if db == nil {
		return nil, errors.New("ConnectionPool must be initialized")
	}

	return &Models{
		DB:                    db,
		Account:               &AccountModel{DB: db, MetricsService: metricsService},
		Contract:              &ContractModel{DB: db, MetricsService: metricsService},
		TrustlineAsset:        &TrustlineAssetModel{DB: db, MetricsService: metricsService},
		TrustlineBalance:      &TrustlineBalanceModel{DB: db, MetricsService: metricsService},
		AccountContractTokens: &AccountContractTokensModel{DB: db, MetricsService: metricsService},
		IngestStore:           &IngestStoreModel{DB: db, MetricsService: metricsService},
		Operations:            &OperationModel{DB: db, MetricsService: metricsService},
		Transactions:          &TransactionModel{DB: db, MetricsService: metricsService},
		StateChanges:          &StateChangeModel{DB: db, MetricsService: metricsService},
	}, nil
}
