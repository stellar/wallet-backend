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

const (
	operationColumns = "id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at"
)

type OperationModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

func (m *OperationModel) GetAll(ctx context.Context, limit *int32, columns string, after *int64) ([]*types.OperationWithCursor, error) {
	if columns == "" {
		columns = operationColumns
	}
	query := fmt.Sprintf(`SELECT %s, operations.id as op_cursor FROM operations`, columns)

	if after != nil {
		query += fmt.Sprintf(` WHERE id < %d`, *after)
	}
	query += ` ORDER BY id DESC`

	if limit != nil && *limit > 0 {
		// Fetch one more item to check if there's a next page.
		query += fmt.Sprintf(` LIMIT %d`, *limit+1)
	}

	var operations []*types.OperationWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &operations, query)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "operations", duration)
	if err != nil {
		return nil, fmt.Errorf("getting all operations: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "operations")
	return operations, nil
}

// BatchGetByTxHashes gets the operations that are associated with the given transaction hashes.
func (m *OperationModel) BatchGetByTxHashes(ctx context.Context, txHashes []string, columns string, limit *int32, cursors []*int64) ([]*types.OperationWithCursor, error) {
	if columns == "" {
		columns = operationColumns
	}

	query := `
		WITH
			inputs (tx_hash, cursor) AS (
				SELECT * FROM UNNEST($1::text[], $2::bigint[])
			),
			
			ranked_operations_per_tx_hash AS (
				SELECT
					o.id,
					o.tx_hash,
					o.operation_type,
					o.operation_xdr,
					o.ledger_number,
					o.ledger_created_at,
					ROW_NUMBER() OVER (PARTITION BY o.tx_hash ORDER BY o.id DESC) AS rn
				FROM 
					operations o
				JOIN 
					inputs i ON o.tx_hash = i.tx_hash
				WHERE 
					(i.cursor IS NULL OR o.id < i.cursor)
			)
		SELECT %s, tx_hash, id as op_cursor FROM ranked_operations_per_tx_hash
	`
	query = fmt.Sprintf(query, columns)

	if limit != nil && *limit > 0 {
		query += fmt.Sprintf(` WHERE rn <= %d`, *limit)
	}

	var operations []*types.OperationWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &operations, query, pq.Array(txHashes), pq.Array(cursors))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "operations", duration)
	if err != nil {
		return nil, fmt.Errorf("getting operations by tx hashes: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "operations")
	return operations, nil
}

// BatchGetByAccountAddress gets the operations that are associated with the given account addresses.
func (m *OperationModel) BatchGetByAccountAddress(ctx context.Context, accountAddress string, columns string, limit *int32, after *int64) ([]*types.OperationWithCursor, error) {
	if columns == "" {
		columns = "operations.*"
	}
	query := fmt.Sprintf(`
		SELECT %s, operations.id as op_cursor
		FROM operations 
		INNER JOIN operations_accounts 
		ON operations.id = operations_accounts.operation_id 
		WHERE operations_accounts.account_id = $1
	`, columns)

	if after != nil {
		query += fmt.Sprintf(` AND operations.id < %d`, *after)
	}
	query += ` ORDER BY operations.id DESC`

	if limit != nil && *limit > 0 {
		// Fetch one more item to check if there's a next page.
		query += fmt.Sprintf(` LIMIT %d`, *limit+1)
	}

	var operations []*types.OperationWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &operations, query, accountAddress)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "operations", duration)
	if err != nil {
		return nil, fmt.Errorf("getting operations for account %s: %w", accountAddress, err)
	}
	m.MetricsService.IncDBQuery("SELECT", "operations")

	return operations, nil
}

// BatchGetByStateChangeIDs gets the operations that are associated with the given state change IDs.
func (m *OperationModel) BatchGetByStateChangeIDs(ctx context.Context, stateChangeIDs []string, columns string) ([]*types.OperationWithStateChangeID, error) {
	if columns == "" {
		columns = "operations.*"
	}
	query := fmt.Sprintf(`
		SELECT %s, state_changes.id AS state_change_id
		FROM operations
		INNER JOIN state_changes ON operations.id = state_changes.operation_id
		WHERE state_changes.id = ANY($1)
		ORDER BY operations.ledger_created_at DESC
	`, columns)
	var operationsWithStateChanges []*types.OperationWithStateChangeID
	start := time.Now()
	err := m.DB.SelectContext(ctx, &operationsWithStateChanges, query, pq.Array(stateChangeIDs))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "operations", duration)
	if err != nil {
		return nil, fmt.Errorf("getting operations by state change IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "operations")
	return operationsWithStateChanges, nil
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
	ledgerNumbers := make([]uint32, len(operations))
	ledgerCreatedAts := make([]time.Time, len(operations))

	for i, op := range operations {
		ids[i] = op.ID
		txHashes[i] = op.TxHash
		operationTypes[i] = string(op.OperationType)
		operationXDRs[i] = op.OperationXDR
		ledgerNumbers[i] = op.LedgerNumber
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
		SELECT stellar_address FROM accounts WHERE stellar_address=ANY($8)
	),

	-- STEP 2: Get operation IDs to insert (connected to at least one existing account)
    valid_operations AS (
        SELECT DISTINCT op_id
        FROM (
			SELECT
				UNNEST($7::bigint[]) AS op_id,
				UNNEST($8::text[]) AS account_id
		) oa
		WHERE oa.account_id IN (SELECT stellar_address FROM existing_accounts)
    ),

	-- STEP 3: Insert those operations
    inserted_operations AS (
        INSERT INTO operations
          	(id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at)
        SELECT
            vo.op_id,
            o.tx_hash,
            o.operation_type,
            o.operation_xdr,
			o.ledger_number,
            o.ledger_created_at
        FROM valid_operations vo
        JOIN (
            SELECT
                UNNEST($1::bigint[]) AS id,
                UNNEST($2::text[]) AS tx_hash,
                UNNEST($3::text[]) AS operation_type,
                UNNEST($4::text[]) AS operation_xdr,
                UNNEST($5::bigint[]) AS ledger_number,
                UNNEST($6::timestamptz[]) AS ledger_created_at
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
				UNNEST($7::bigint[]) AS op_id,
				UNNEST($8::text[]) AS account_id
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
		pq.Array(ledgerNumbers),
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
