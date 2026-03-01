package data

import (
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/metrics"
)

type Models struct {
	DB                    *pgxpool.Pool
	Account               *AccountModel
	Contract              ContractModelInterface
	TrustlineAsset        TrustlineAssetModelInterface
	TrustlineBalance      TrustlineBalanceModelInterface
	NativeBalance         NativeBalanceModelInterface
	SACBalance            SACBalanceModelInterface
	AccountContractTokens AccountContractTokensModelInterface
	IngestStore           *IngestStoreModel
	Operations            *OperationModel
	Transactions          *TransactionModel
	StateChanges          *StateChangeModel
}

func NewModels(pool *pgxpool.Pool, metricsService metrics.MetricsService) (*Models, error) {
	if pool == nil {
		return nil, errors.New("DB pool must be initialized")
	}

	return &Models{
		DB:                    pool,
		Account:               &AccountModel{DB: pool, MetricsService: metricsService},
		Contract:              &ContractModel{DB: pool, MetricsService: metricsService},
		TrustlineAsset:        &TrustlineAssetModel{DB: pool, MetricsService: metricsService},
		TrustlineBalance:      &TrustlineBalanceModel{DB: pool, MetricsService: metricsService},
		NativeBalance:         &NativeBalanceModel{DB: pool, MetricsService: metricsService},
		SACBalance:            &SACBalanceModel{DB: pool, MetricsService: metricsService},
		AccountContractTokens: &AccountContractTokensModel{DB: pool, MetricsService: metricsService},
		IngestStore:           &IngestStoreModel{DB: pool, MetricsService: metricsService},
		Operations:            &OperationModel{DB: pool, MetricsService: metricsService},
		Transactions:          &TransactionModel{DB: pool, MetricsService: metricsService},
		StateChanges:          &StateChangeModel{DB: pool, MetricsService: metricsService},
	}, nil
}
