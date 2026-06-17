package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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
	GetByProtocolID(ctx context.Context, protocolID string) ([]ProtocolContracts, error)
	BatchGetByProtocolIDs(ctx context.Context, protocolIDs []string) (map[string][]ProtocolContracts, error)
	BatchGetByContractIDs(ctx context.Context, contractIDs []types.HashBytea) (map[string][]ProtocolContracts, error)
	GetByWasmHashes(ctx context.Context, wasmHashes []types.HashBytea) ([]ProtocolContracts, error)
}

// ProtocolContractsModel implements ProtocolContractsModelInterface.
type ProtocolContractsModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
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
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchInsert", "protocol_contracts").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchInsert", "protocol_contracts").Observe(float64(len(contracts)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchInsert", "protocol_contracts").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchInsert", "protocol_contracts", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("batch inserting protocol contracts: %w", err)
	}
	return nil
}

// GetByProtocolID returns all contracts associated with a protocol via protocol_wasms.
func (m *ProtocolContractsModel) GetByProtocolID(ctx context.Context, protocolID string) ([]ProtocolContracts, error) {
	const query = `
		SELECT pc.contract_id, pc.wasm_hash, pc.name, pc.created_at
		FROM protocol_contracts pc
		JOIN protocol_wasms pw ON pc.wasm_hash = pw.wasm_hash
		WHERE pw.protocol_id = $1
	`

	start := time.Now()
	contracts, err := db.QueryMany[ProtocolContracts](ctx, m.DB, query, protocolID)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByProtocolID", "protocol_contracts").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByProtocolID", "protocol_contracts").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByProtocolID", "protocol_contracts", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying contracts for protocol %s: %w", protocolID, err)
	}
	return contracts, nil
}

// GetByWasmHashes returns protocol_contracts rows whose wasm_hash is in the given set.
func (m *ProtocolContractsModel) GetByWasmHashes(ctx context.Context, wasmHashes []types.HashBytea) ([]ProtocolContracts, error) {
	if len(wasmHashes) == 0 {
		return nil, nil
	}
	hashBytes := make([][]byte, len(wasmHashes))
	for i, h := range wasmHashes {
		val, err := h.Value()
		if err != nil {
			return nil, fmt.Errorf("converting wasm hash to bytes: %w", err)
		}
		hashBytes[i] = val.([]byte)
	}

	const query = `SELECT contract_id, wasm_hash, name, created_at FROM protocol_contracts WHERE wasm_hash = ANY($1::bytea[])`

	start := time.Now()
	contracts, err := db.QueryMany[ProtocolContracts](ctx, m.DB, query, hashBytes)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByWasmHashes", "protocol_contracts").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByWasmHashes", "protocol_contracts").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByWasmHashes", "protocol_contracts", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying protocol_contracts by wasm hashes: %w", err)
	}
	return contracts, nil
}

// BatchGetByProtocolIDs returns all contracts for the given protocol IDs in a single query,
// grouped by protocol ID. This avoids N+1 queries when refreshing the contract cache.
func (m *ProtocolContractsModel) BatchGetByProtocolIDs(ctx context.Context, protocolIDs []string) (map[string][]ProtocolContracts, error) {
	if len(protocolIDs) == 0 {
		return nil, nil
	}

	const query = `
		SELECT pw.protocol_id, pc.contract_id, pc.wasm_hash, pc.name, pc.created_at
		FROM protocol_contracts pc
		JOIN protocol_wasms pw ON pc.wasm_hash = pw.wasm_hash
		WHERE pw.protocol_id = ANY($1)
	`

	start := time.Now()
	rows, err := m.DB.Query(ctx, query, protocolIDs)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByProtocolIDs", "protocol_contracts").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByProtocolIDs", "protocol_contracts").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByProtocolIDs", "protocol_contracts", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("batch querying contracts for protocols: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]ProtocolContracts, len(protocolIDs))
	for rows.Next() {
		var protocolID string
		var c ProtocolContracts
		if err := rows.Scan(&protocolID, &c.ContractID, &c.WasmHash, &c.Name, &c.CreatedAt); err != nil {
			m.Metrics.QueryErrors.WithLabelValues("BatchGetByProtocolIDs", "protocol_contracts", utils.GetDBErrorType(err)).Inc()
			return nil, fmt.Errorf("scanning batch protocol contract row: %w", err)
		}
		result[protocolID] = append(result[protocolID], c)
	}
	if err := rows.Err(); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByProtocolIDs", "protocol_contracts", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("iterating batch protocol contract rows: %w", err)
	}
	return result, nil
}

// BatchGetByContractIDs returns, for the given contract IDs, the protocol_contracts
// rows whose wasm is classified to a protocol, grouped by protocol ID. Live ingestion
// uses this to resolve membership for only the contracts that emitted events in a
// ledger, instead of loading every committed contract for a protocol.
func (m *ProtocolContractsModel) BatchGetByContractIDs(ctx context.Context, contractIDs []types.HashBytea) (map[string][]ProtocolContracts, error) {
	if len(contractIDs) == 0 {
		return nil, nil
	}

	idBytes := make([][]byte, len(contractIDs))
	for i, id := range contractIDs {
		val, err := id.Value()
		if err != nil {
			return nil, fmt.Errorf("converting contract id to bytes: %w", err)
		}
		idBytes[i] = val.([]byte)
	}

	const query = `
		SELECT pw.protocol_id, pc.contract_id, pc.wasm_hash, pc.name, pc.created_at
		FROM protocol_contracts pc
		JOIN protocol_wasms pw ON pc.wasm_hash = pw.wasm_hash
		WHERE pc.contract_id = ANY($1::bytea[]) AND pw.protocol_id IS NOT NULL
	`

	start := time.Now()
	rows, err := m.DB.Query(ctx, query, idBytes)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByContractIDs", "protocol_contracts").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByContractIDs", "protocol_contracts").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByContractIDs", "protocol_contracts", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("batch querying contracts by ids: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]ProtocolContracts, len(contractIDs))
	for rows.Next() {
		var protocolID string
		var c ProtocolContracts
		if err := rows.Scan(&protocolID, &c.ContractID, &c.WasmHash, &c.Name, &c.CreatedAt); err != nil {
			m.Metrics.QueryErrors.WithLabelValues("BatchGetByContractIDs", "protocol_contracts", utils.GetDBErrorType(err)).Inc()
			return nil, fmt.Errorf("scanning protocol contract row by id: %w", err)
		}
		result[protocolID] = append(result[protocolID], c)
	}
	if err := rows.Err(); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByContractIDs", "protocol_contracts", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("iterating protocol contract rows by id: %w", err)
	}
	return result, nil
}
