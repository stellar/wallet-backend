package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// ProtocolContracts represents a contract tracked during checkpoint population,
// linking a contract_id to its WASM hash.
type ProtocolContracts struct {
	ContractID types.HashBytea `db:"contract_id"`
	WasmHash   types.HashBytea `db:"wasm_hash"`
	Name       *string         `db:"name"`
	CreatedAt  time.Time       `db:"created_at"`
}

// ProtocolContractsModelInterface defines the interface for protocol_contracts operations.
type ProtocolContractsModelInterface interface {
	BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []ProtocolContracts) error
}

// ProtocolContractsModel implements ProtocolContractsModelInterface.
type ProtocolContractsModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

var _ ProtocolContractsModelInterface = (*ProtocolContractsModel)(nil)

// BatchInsert inserts or updates multiple protocol contracts using UNNEST for efficient batch insertion.
// Uses ON CONFLICT (contract_id) DO UPDATE to support contract upgrades (last-write-wins).
func (m *ProtocolContractsModel) BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []ProtocolContracts) error {
	if len(contracts) == 0 {
		return nil
	}

	contractIDs := make([][]byte, len(contracts))
	wasmHashes := make([][]byte, len(contracts))
	names := make([]*string, len(contracts))

	for i, c := range contracts {
		cidVal, err := c.ContractID.Value()
		if err != nil {
			return fmt.Errorf("converting contract id to bytes: %w", err)
		}
		contractIDs[i] = cidVal.([]byte)

		whVal, err := c.WasmHash.Value()
		if err != nil {
			return fmt.Errorf("converting wasm hash to bytes: %w", err)
		}
		wasmHashes[i] = whVal.([]byte)
		names[i] = c.Name
	}

	const query = `
		INSERT INTO protocol_contracts (contract_id, wasm_hash, name)
		SELECT u.contract_id, u.wasm_hash, u.name
		FROM UNNEST($1::bytea[], $2::bytea[], $3::text[])
			AS u(contract_id, wasm_hash, name)
		WHERE EXISTS (SELECT 1 FROM protocol_wasms pw WHERE pw.wasm_hash = u.wasm_hash)
		ON CONFLICT (contract_id) DO UPDATE SET
			wasm_hash = EXCLUDED.wasm_hash,
			name = COALESCE(EXCLUDED.name, protocol_contracts.name)
	`

	start := time.Now()
	_, err := dbTx.Exec(ctx, query, contractIDs, wasmHashes, names)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchInsert", "protocol_contracts", utils.GetDBErrorType(err))
		return fmt.Errorf("batch inserting protocol contracts: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchInsert", "protocol_contracts", time.Since(start).Seconds())
	m.MetricsService.ObserveDBBatchSize("BatchInsert", "protocol_contracts", len(contracts))
	m.MetricsService.IncDBQuery("BatchInsert", "protocol_contracts")
	return nil
}
