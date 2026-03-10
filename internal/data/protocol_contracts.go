package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// ProtocolContract represents a contract tracked during checkpoint population,
// linking a contract_id to its WASM hash.
type ProtocolContract struct {
	ContractID []byte    `db:"contract_id"`
	WasmHash   []byte    `db:"wasm_hash"`
	Name       *string   `db:"name"`
	CreatedAt  time.Time `db:"created_at"`
}

// ProtocolContractModelInterface defines the interface for protocol_contracts operations.
type ProtocolContractModelInterface interface {
	BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []ProtocolContract) error
}

// ProtocolContractModel implements ProtocolContractModelInterface.
type ProtocolContractModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

var _ ProtocolContractModelInterface = (*ProtocolContractModel)(nil)

// BatchInsert inserts multiple protocol contracts using UNNEST for efficient batch insertion.
// Uses ON CONFLICT (contract_id) DO NOTHING for idempotent operations.
func (m *ProtocolContractModel) BatchInsert(ctx context.Context, dbTx pgx.Tx, contracts []ProtocolContract) error {
	if len(contracts) == 0 {
		return nil
	}

	contractIDs := make([][]byte, len(contracts))
	wasmHashes := make([][]byte, len(contracts))
	names := make([]*string, len(contracts))

	for i, c := range contracts {
		contractIDs[i] = c.ContractID
		wasmHashes[i] = c.WasmHash
		names[i] = c.Name
	}

	const query = `
		INSERT INTO protocol_contracts (contract_id, wasm_hash, name)
		SELECT u.contract_id, u.wasm_hash, u.name
		FROM UNNEST($1::bytea[], $2::bytea[], $3::text[])
			AS u(contract_id, wasm_hash, name)
		WHERE EXISTS (SELECT 1 FROM protocol_wasms pw WHERE pw.wasm_hash = u.wasm_hash)
		ON CONFLICT (contract_id) DO NOTHING
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
