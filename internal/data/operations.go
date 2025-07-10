package data

import (
	"context"
	"fmt"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

type OperationModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

func (m *OperationModel) BatchGetByTxHash(ctx context.Context, txHashes []string) ([]*types.Operation, error) {
	const query = `SELECT * FROM operations WHERE tx_hash = ANY($1)`
	var operations []*types.Operation
	start := time.Now()
	err := m.DB.SelectContext(ctx, &operations, query, pq.Array(txHashes))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "operations", duration)
	if err != nil {
		return nil, fmt.Errorf("getting operations by tx hashes: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "operations")
	return operations, nil
}

// BatchInsert inserts the operations and the operations_accounts links.
// It returns the IDs of the successfully inserted operations.
func (m *OperationModel) BatchInsert(
	ctx context.Context,
	sqlExecuter db.SQLExecuter,
	operations []types.Operation,
	stellarAddressesByOpID map[int64]set.Set[string],
) ([]int64, error) {
	if sqlExecuter == nil {
		sqlExecuter = m.DB
	}

	// 1. Flatten the operations into parallel slices
	ids := make([]int64, len(operations))
	txHashes := make([]string, len(operations))
	operationTypes := make([]string, len(operations))
	operationXDRs := make([]string, len(operations))
	ledgerCreatedAts := make([]time.Time, len(operations))

	for i, op := range operations {
		ids[i] = op.ID
		txHashes[i] = op.TxHash
		operationTypes[i] = string(op.OperationType)
		operationXDRs[i] = op.OperationXDR
		ledgerCreatedAts[i] = op.LedgerCreatedAt
	}

	// 2. Flatten the stellarAddressesByOpID into parallel slices
	var opIDs []int64
	var stellarAddresses []string
	for opID, addresses := range stellarAddressesByOpID {
		for address := range addresses.Iter() {
			opIDs = append(opIDs, opID)
			stellarAddresses = append(stellarAddresses, address)
		}
	}

	// 3. Single query that inserts only operations that are connected to at least one existing account.
	// It also inserts the operations_accounts links.
	const insertQuery = `
	WITH
	-- STEP 1: Get existing accounts
	existing_accounts AS (
		SELECT stellar_address FROM accounts WHERE stellar_address=ANY($7)
	),

	-- STEP 2: Get operation IDs to insert (connected to at least one existing account)
    valid_operations AS (
        SELECT DISTINCT op_id
        FROM (
			SELECT
				UNNEST($6::bigint[]) AS op_id,
				UNNEST($7::text[]) AS account_id
		) oa
		WHERE oa.account_id IN (SELECT stellar_address FROM existing_accounts)
    ),

	-- STEP 3: Insert those operations
    inserted_operations AS (
        INSERT INTO operations
          	(id, tx_hash, operation_type, operation_xdr, ledger_created_at)
        SELECT
            vo.op_id,
            o.tx_hash,
            o.operation_type,
            o.operation_xdr,
            o.ledger_created_at
        FROM valid_operations vo
        JOIN (
            SELECT
                UNNEST($1::bigint[]) AS id,
                UNNEST($2::text[]) AS tx_hash,
                UNNEST($3::text[]) AS operation_type,
                UNNEST($4::text[]) AS operation_xdr,
                UNNEST($5::timestamptz[]) AS ledger_created_at
        ) o ON o.id = vo.op_id
        ON CONFLICT (id) DO NOTHING
        RETURNING id
    ),

	-- STEP 4: Insert operations_accounts links
	inserted_operations_accounts AS (
		INSERT INTO operations_accounts
			(operation_id, account_id)
		SELECT
			oa.op_id, oa.account_id
		FROM (
			SELECT
				UNNEST($6::bigint[]) AS op_id,
				UNNEST($7::text[]) AS account_id
		) oa
		INNER JOIN existing_accounts ea ON ea.stellar_address = oa.account_id
		INNER JOIN inserted_operations io ON io.id = oa.op_id
		ON CONFLICT DO NOTHING
	)

	-- STEP 5: Return the IDs of successfully inserted operations
	SELECT id FROM inserted_operations;
    `

	start := time.Now()
	var insertedIDs []int64
	err := sqlExecuter.SelectContext(ctx, &insertedIDs, insertQuery,
		pq.Array(ids),
		pq.Array(txHashes),
		pq.Array(operationTypes),
		pq.Array(operationXDRs),
		pq.Array(ledgerCreatedAts),
		pq.Array(opIDs),
		pq.Array(stellarAddresses),
	)
	duration := time.Since(start).Seconds()
	for _, dbTableName := range []string{"operations", "operations_accounts"} {
		m.MetricsService.ObserveDBQueryDuration("INSERT", dbTableName, duration)
		if err == nil {
			m.MetricsService.IncDBQuery("INSERT", dbTableName)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("batch inserting operations and operations_accounts: %w", err)
	}

	return insertedIDs, nil
}
