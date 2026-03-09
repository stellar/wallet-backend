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
	ContractID string    `db:"contract_id"`
	WasmHash   string    `db:"wasm_hash"`
	ProtocolID *string   `db:"protocol_id"`
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

	contractIDs := make([]string, len(contracts))
	wasmHashes := make([]string, len(contracts))
	protocolIDs := make([]*string, len(contracts))
	names := make([]*string, len(contracts))

	for i, c := range contracts {
		contractIDs[i] = c.ContractID
		wasmHashes[i] = c.WasmHash
		protocolIDs[i] = c.ProtocolID
		names[i] = c.Name
	}

	const query = `
		INSERT INTO protocol_contracts (contract_id, wasm_hash, protocol_id, name)
		SELECT u.contract_id, u.wasm_hash, u.protocol_id, u.name
		FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[])
			AS u(contract_id, wasm_hash, protocol_id, name)
		WHERE EXISTS (SELECT 1 FROM protocol_wasms pw WHERE pw.wasm_hash = u.wasm_hash)
		ON CONFLICT (contract_id) DO NOTHING
	`

	start := time.Now()
	_, err := dbTx.Exec(ctx, query, contractIDs, wasmHashes, protocolIDs, names)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchInsert", "protocol_contracts", utils.GetDBErrorType(err))
		return fmt.Errorf("batch inserting protocol contracts: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchInsert", "protocol_contracts", time.Since(start).Seconds())
	m.MetricsService.ObserveDBBatchSize("BatchInsert", "protocol_contracts", len(contracts))
	m.MetricsService.IncDBQuery("BatchInsert", "protocol_contracts")
	return nil
}
