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

// ProtocolMigrateHistoryService backfills protocol state changes for historical ledgers.
type ProtocolMigrateHistoryService interface {
	Run(ctx context.Context, protocolIDs []string) error
}

var _ ProtocolMigrateHistoryService = (*protocolMigrateHistoryService)(nil)

type protocolMigrateHistoryService struct {
	engine protocolMigrateEngine
}

// ProtocolMigrateHistoryConfig holds the configuration for creating a protocolMigrateHistoryService.
type ProtocolMigrateHistoryConfig struct {
	DB                     *pgxpool.Pool
	LedgerBackend          ledgerbackend.LedgerBackend
	ProtocolsModel         data.ProtocolsModelInterface
	ProtocolContractsModel data.ProtocolContractsModelInterface
	IngestStore            *data.IngestStoreModel
	NetworkPassphrase      string
	Processors             []ProtocolProcessor
	OldestLedgerCursorName string
}

// NewProtocolMigrateHistoryService creates a new protocolMigrateHistoryService from the given config.
func NewProtocolMigrateHistoryService(cfg ProtocolMigrateHistoryConfig) (*protocolMigrateHistoryService, error) {
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

	oldestCursor := cfg.OldestLedgerCursorName
	if oldestCursor == "" {
		oldestCursor = data.OldestLedgerCursorName
	}

	ingestStore := cfg.IngestStore

	return &protocolMigrateHistoryService{
		engine: protocolMigrateEngine{
			db:                     cfg.DB,
			ledgerBackend:          cfg.LedgerBackend,
			protocolsModel:         cfg.ProtocolsModel,
			protocolContractsModel: cfg.ProtocolContractsModel,
			ingestStore:            cfg.IngestStore,
			networkPassphrase:      cfg.NetworkPassphrase,
			processors:             ppMap,
			strategy: migrationStrategy{
				Label:                 "history",
				UpdateMigrationStatus: cfg.ProtocolsModel.UpdateHistoryMigrationStatus,
				MigrationStatusField:  func(p *data.Protocols) string { return p.HistoryMigrationStatus },
				CursorName:            utils.ProtocolHistoryCursorName,
				Persist: func(ctx context.Context, dbTx pgx.Tx, proc ProtocolProcessor) error {
					return proc.PersistHistory(ctx, dbTx)
				},
				ResolveStartLedger: func(ctx context.Context) (uint32, error) {
					v, err := ingestStore.Get(ctx, oldestCursor)
					if err != nil {
						return 0, fmt.Errorf("reading oldest ingest ledger: %w", err)
					}
					if v == 0 {
						return 0, fmt.Errorf("ingestion has not started yet (oldest_ingest_ledger is 0)")
					}
					return v, nil
				},
			},
		},
	}, nil
}

// Run performs history migration for the given protocol IDs.
func (s *protocolMigrateHistoryService) Run(ctx context.Context, protocolIDs []string) error {
	return s.engine.Run(ctx, protocolIDs)
}
