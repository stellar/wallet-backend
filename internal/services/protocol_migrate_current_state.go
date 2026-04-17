package services

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/utils"
)

// ProtocolMigrateCurrentStateService builds protocol current state from a start ledger forward.
type ProtocolMigrateCurrentStateService interface {
	Run(ctx context.Context, protocolIDs []string) error
}

var _ ProtocolMigrateCurrentStateService = (*protocolMigrateCurrentStateService)(nil)

type protocolMigrateCurrentStateService struct {
	engine protocolMigrateEngine
}

// ProtocolMigrateCurrentStateConfig holds the configuration for creating a protocolMigrateCurrentStateService.
type ProtocolMigrateCurrentStateConfig struct {
	DB                     *pgxpool.Pool
	LedgerBackend          ledgerbackend.LedgerBackend
	ProtocolsModel         data.ProtocolsModelInterface
	ProtocolContractsModel data.ProtocolContractsModelInterface
	IngestStore            *data.IngestStoreModel
	NetworkPassphrase      string
	Processors             []ProtocolProcessor
	StartLedger            uint32
}

// NewProtocolMigrateCurrentStateService creates a new protocolMigrateCurrentStateService from the given config.
func NewProtocolMigrateCurrentStateService(cfg ProtocolMigrateCurrentStateConfig) (*protocolMigrateCurrentStateService, error) {
	if cfg.StartLedger == 0 {
		return nil, fmt.Errorf("start ledger must be > 0")
	}

	for i, p := range cfg.Processors {
		if p == nil {
			return nil, fmt.Errorf("protocol processor at index %d is nil", i)
		}
	}
	ppMap, err := utils.BuildMap(cfg.Processors, func(p ProtocolProcessor) string {
		return p.ProtocolID()
	})
	if err != nil {
		return nil, fmt.Errorf("building protocol processor map: %w", err)
	}

	startLedger := cfg.StartLedger

	return &protocolMigrateCurrentStateService{
		engine: protocolMigrateEngine{
			db:                     cfg.DB,
			ledgerBackend:          cfg.LedgerBackend,
			protocolsModel:         cfg.ProtocolsModel,
			protocolContractsModel: cfg.ProtocolContractsModel,
			ingestStore:            cfg.IngestStore,
			networkPassphrase:      cfg.NetworkPassphrase,
			processors:             ppMap,
			strategy: migrationStrategy{
				Label:                 "current state",
				UpdateMigrationStatus: cfg.ProtocolsModel.UpdateCurrentStateMigrationStatus,
				MigrationStatusField:  func(p *data.Protocols) string { return p.CurrentStateMigrationStatus },
				CursorName:            utils.ProtocolCurrentStateCursorName,
				Persist: func(ctx context.Context, dbTx pgx.Tx, proc ProtocolProcessor) error {
					return proc.PersistCurrentState(ctx, dbTx)
				},
				ResolveStartLedger: func(_ context.Context) (uint32, error) {
					return startLedger, nil
				},
			},
		},
	}, nil
}

// Run performs current-state migration for the given protocol IDs.
func (s *protocolMigrateCurrentStateService) Run(ctx context.Context, protocolIDs []string) error {
	return s.engine.Run(ctx, protocolIDs)
}
