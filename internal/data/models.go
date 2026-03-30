package data

import (
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/metrics"
)

type Models struct {
	DB                *pgxpool.Pool
	Account           *AccountModel
	Contract          ContractModelInterface
	TrustlineAsset    TrustlineAssetModelInterface
	TrustlineBalance  TrustlineBalanceModelInterface
	NativeBalance     NativeBalanceModelInterface
	SACBalance        SACBalanceModelInterface
	ProtocolWasms     ProtocolWasmsModelInterface
	Protocols         ProtocolsModelInterface
	ProtocolContracts ProtocolContractsModelInterface
	IngestStore       *IngestStoreModel
	Operations        *OperationModel
	Transactions      *TransactionModel
	StateChanges      *StateChangeModel
}

func NewModels(pool *pgxpool.Pool, dbMetrics *metrics.DBMetrics) (*Models, error) {
	if pool == nil {
		return nil, errors.New("DB pool must be initialized")
	}

	return &Models{
		DB:                pool,
		Account:           &AccountModel{DB: pool, Metrics: dbMetrics},
		Contract:          &ContractModel{DB: pool, Metrics: dbMetrics},
		TrustlineAsset:    &TrustlineAssetModel{DB: pool, Metrics: dbMetrics},
		TrustlineBalance:  &TrustlineBalanceModel{DB: pool, Metrics: dbMetrics},
		NativeBalance:     &NativeBalanceModel{DB: pool, Metrics: dbMetrics},
		SACBalance:        &SACBalanceModel{DB: pool, Metrics: dbMetrics},
		ProtocolWasms:     &ProtocolWasmsModel{DB: pool, Metrics: dbMetrics},
		Protocols:         &ProtocolsModel{DB: pool, Metrics: dbMetrics},
		ProtocolContracts: &ProtocolContractsModel{DB: pool, Metrics: dbMetrics},
		IngestStore:       &IngestStoreModel{DB: pool, Metrics: dbMetrics},
		Operations:        &OperationModel{DB: pool, Metrics: dbMetrics},
		Transactions:      &TransactionModel{DB: pool, Metrics: dbMetrics},
		StateChanges:      &StateChangeModel{DB: pool, Metrics: dbMetrics},
	}, nil
}
