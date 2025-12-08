package data

import (
	"context"
	"fmt"
	"strings"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type OperationModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

func (m *OperationModel) GetByID(ctx context.Context, id int64, columns string) (*types.Operation, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "", "id")
	query := fmt.Sprintf(`SELECT %s FROM operations WHERE id = $1`, columns)
	var operation types.Operation
	start := time.Now()
	err := m.DB.GetContext(ctx, &operation, query, id)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetByID", "operations", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetByID", "operations", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting operation by id: %w", err)
	}
	m.MetricsService.IncDBQuery("GetByID", "operations")
	return &operation, nil
}

func (m *OperationModel) GetAll(ctx context.Context, columns string, limit *int32, cursor *int64, sortOrder SortOrder) ([]*types.OperationWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "", "id")
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString(fmt.Sprintf(`SELECT %s, id as cursor FROM operations`, columns))

	if cursor != nil {
		if sortOrder == DESC {
			queryBuilder.WriteString(fmt.Sprintf(" WHERE id < %d", *cursor))
		} else {
			queryBuilder.WriteString(fmt.Sprintf(" WHERE id > %d", *cursor))
		}
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY id DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY id ASC")
	}

	if limit != nil {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT %d", *limit))
	}
	query := queryBuilder.String()
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS operations ORDER BY cursor ASC`, query)
	}

	var operations []*types.OperationWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &operations, query)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetAll", "operations", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetAll", "operations", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting operations: %w", err)
	}
	m.MetricsService.IncDBQuery("GetAll", "operations")
	return operations, nil
}

// BatchGetByTxHashes gets the operations that are associated with the given transaction hashes.
func (m *OperationModel) BatchGetByTxHashes(ctx context.Context, txHashes []string, columns string, limit *int32, sortOrder SortOrder) ([]*types.OperationWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "", "id")
	queryBuilder := strings.Builder{}
	// This CTE query implements per-transaction pagination to ensure balanced results.
	// Instead of applying a global LIMIT that could return all operations from just a few
	// transactions, we use ROW_NUMBER() with PARTITION BY tx_hash to limit results per transaction.
	// This guarantees that each transaction gets at most 'limit' operations, providing
	// more balanced and predictable pagination across multiple transactions.
	query := `
		WITH
			inputs (tx_hash) AS (
				SELECT * FROM UNNEST($1::text[])
			),
			
			ranked_operations_per_tx_hash AS (
				SELECT
					o.*,
					ROW_NUMBER() OVER (PARTITION BY o.tx_hash ORDER BY o.id %s) AS rn
				FROM 
					operations o
				JOIN 
					inputs i ON o.tx_hash = i.tx_hash
			)
		SELECT %s, id as cursor FROM ranked_operations_per_tx_hash
	`
	queryBuilder.WriteString(fmt.Sprintf(query, sortOrder, columns))
	if limit != nil {
		queryBuilder.WriteString(fmt.Sprintf(" WHERE rn <= %d", *limit))
	}
	query = queryBuilder.String()
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS operations ORDER BY cursor ASC`, query)
	}

	var operations []*types.OperationWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &operations, query, pq.Array(txHashes))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByTxHashes", "operations", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByTxHashes", "operations", len(txHashes))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByTxHashes", "operations", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting operations by tx hashes: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByTxHashes", "operations")
	return operations, nil
}

// BatchGetByTxHash gets operations for a single transaction with pagination support.
func (m *OperationModel) BatchGetByTxHash(ctx context.Context, txHash string, columns string, limit *int32, cursor *int64, sortOrder SortOrder) ([]*types.OperationWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "", "id")
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString(fmt.Sprintf(`SELECT %s, id as cursor FROM operations WHERE tx_hash = $1`, columns))

	args := []interface{}{txHash}
	argIndex := 2

	if cursor != nil {
		if sortOrder == DESC {
			queryBuilder.WriteString(fmt.Sprintf(" AND id < $%d", argIndex))
		} else {
			queryBuilder.WriteString(fmt.Sprintf(" AND id > $%d", argIndex))
		}
		args = append(args, *cursor)
		argIndex++
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY id DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY id ASC")
	}

	if limit != nil {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT $%d", argIndex))
		args = append(args, *limit)
	}

	query := queryBuilder.String()
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS operations ORDER BY cursor ASC`, query)
	}

	var operations []*types.OperationWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &operations, query, args...)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByTxHash", "operations", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByTxHash", "operations", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting paginated operations by tx hash: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByTxHash", "operations")
	return operations, nil
}

// BatchGetByAccountAddress gets the operations that are associated with a single account address.
func (m *OperationModel) BatchGetByAccountAddress(ctx context.Context, accountAddress string, columns string, limit *int32, cursor *int64, orderBy SortOrder) ([]*types.OperationWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "operations", "id")

	// Build paginated query using shared utility
	query, args := buildGetByAccountAddressQuery(paginatedQueryConfig{
		TableName:      "operations",
		CursorColumn:   "id",
		JoinTable:      "operations_accounts",
		JoinCondition:  "operations_accounts.operation_id = operations.id",
		Columns:        columns,
		AccountAddress: accountAddress,
		Limit:          limit,
		Cursor:         cursor,
		OrderBy:        orderBy,
	})

	var operations []*types.OperationWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &operations, query, args...)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByAccountAddress", "operations", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByAccountAddress", "operations", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting operations by account address: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByAccountAddress", "operations")
	return operations, nil
}

// BatchGetByStateChangeIDs gets the operations that are associated with the given state change IDs.
func (m *OperationModel) BatchGetByStateChangeIDs(ctx context.Context, scToIDs []int64, scOrders []int64, columns string) ([]*types.OperationWithStateChangeID, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "operations", "id")

	// Build tuples for the IN clause. Since (to_id, state_change_order) is the primary key of state_changes,
	// it will be faster to search on this tuple.
	tuples := make([]string, len(scOrders))
	for i := range scOrders {
		tuples[i] = fmt.Sprintf("(%d, %d)", scToIDs[i], scOrders[i])
	}

	query := fmt.Sprintf(`
		SELECT %s, CONCAT(state_changes.to_id, '-', state_changes.state_change_order) AS state_change_id
		FROM operations
		INNER JOIN state_changes ON operations.id = state_changes.operation_id
		WHERE (state_changes.to_id, state_changes.state_change_order) IN (%s)
		ORDER BY operations.ledger_created_at DESC
	`, columns, strings.Join(tuples, ", "))

	var operationsWithStateChanges []*types.OperationWithStateChangeID
	start := time.Now()
	err := m.DB.SelectContext(ctx, &operationsWithStateChanges, query)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByStateChangeIDs", "operations", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByStateChangeIDs", "operations", len(scOrders))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByStateChangeIDs", "operations", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting operations by state change IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByStateChangeIDs", "operations")
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
	operationXDRs := make([][]byte, len(operations))
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

	// 2. Build lookup map for ledger_created_at by operation ID
	ledgerCreatedAtByID := make(map[int64]time.Time, len(operations))
	for _, op := range operations {
		ledgerCreatedAtByID[op.ID] = op.LedgerCreatedAt
	}

	// 3. Flatten the stellarAddressesByOpID into parallel slices
	var opIDs []int64
	var stellarAddresses []string
	var oaLedgerCreatedAts []time.Time
	for opID, addresses := range stellarAddressesByOpID {
		ledgerCreatedAt := ledgerCreatedAtByID[opID]
		for address := range addresses.Iter() {
			opIDs = append(opIDs, opID)
			stellarAddresses = append(stellarAddresses, address)
			oaLedgerCreatedAts = append(oaLedgerCreatedAts, ledgerCreatedAt)
		}
	}

	// Insert operations and operations_accounts links.
	const insertQuery = `
	WITH
	-- Insert operations
	inserted_operations AS (
		INSERT INTO operations
			(id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at)
		SELECT
			o.id, o.tx_hash, o.operation_type, o.operation_xdr, o.ledger_number, o.ledger_created_at
		FROM (
			SELECT
				UNNEST($1::bigint[]) AS id,
				UNNEST($2::text[]) AS tx_hash,
				UNNEST($3::text[]) AS operation_type,
				UNNEST($4::bytea[]) AS operation_xdr,
				UNNEST($5::bigint[]) AS ledger_number,
				UNNEST($6::timestamptz[]) AS ledger_created_at
		) o
		ON CONFLICT (id) DO NOTHING
		RETURNING id
	),

	-- Insert operations_accounts links
	inserted_operations_accounts AS (
		INSERT INTO operations_accounts
			(operation_id, account_id, ledger_created_at)
		SELECT
			oa.op_id, oa.account_id, oa.ledger_created_at
		FROM (
			SELECT
				UNNEST($7::bigint[]) AS op_id,
				UNNEST($8::text[]) AS account_id,
				UNNEST($9::timestamptz[]) AS ledger_created_at
		) oa
		ON CONFLICT (account_id, operation_id) DO NOTHING
	)

	-- Return the IDs of successfully inserted operations
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
		pq.Array(oaLedgerCreatedAts),
	)
	duration := time.Since(start).Seconds()
	for _, dbTableName := range []string{"operations", "operations_accounts"} {
		m.MetricsService.ObserveDBQueryDuration("BatchInsert", dbTableName, duration)
		if dbTableName == "operations" {
			m.MetricsService.ObserveDBBatchSize("BatchInsert", dbTableName, len(operations))
		}
		if err == nil {
			m.MetricsService.IncDBQuery("BatchInsert", dbTableName)
		}
	}
	if err != nil {
		for _, dbTableName := range []string{"operations", "operations_accounts"} {
			m.MetricsService.IncDBQueryError("BatchInsert", dbTableName, utils.GetDBErrorType(err))
		}
		return nil, fmt.Errorf("batch inserting operations and operations_accounts: %w", err)
	}

	return insertedIDs, nil
}

// BatchInsertCopy inserts operations using PostgreSQL COPY protocol for better performance.
// This method is optimized for backfill operations where duplicates are not expected.
// It uses direct COPY to the target tables without conflict handling.
func (m *OperationModel) BatchInsertCopy(
	ctx context.Context,
	dbTx db.Transaction,
	operations []types.Operation,
	stellarAddressesByOpID map[int64]set.Set[string],
) (int, error) {
	if len(operations) == 0 {
		return 0, nil
	}

	// Convert to pointers for the optimized implementation
	opPtrs := make([]*types.Operation, len(operations))
	for i := range operations {
		opPtrs[i] = &operations[i]
	}

	return m.BatchInsertCopyFromPointers(ctx, dbTx, opPtrs, stellarAddressesByOpID)
}

// BatchInsertCopyFromPointers inserts operations using PostgreSQL COPY protocol.
// This variant accepts pointer slices to avoid unnecessary memory copies.
// Optimized for backfill operations with pre-allocated participant lists.
func (m *OperationModel) BatchInsertCopyFromPointers(
	ctx context.Context,
	dbTx db.Transaction,
	operations []*types.Operation,
	stellarAddressesByOpID map[int64]set.Set[string],
) (int, error) {
	if len(operations) == 0 {
		return 0, nil
	}

	// Get the underlying *sql.Tx from sqlx.Tx for pq.CopyIn
	sqlxTx, ok := dbTx.(*sqlx.Tx)
	if !ok {
		return 0, fmt.Errorf("expected *sqlx.Tx, got %T", dbTx)
	}

	start := time.Now()

	// COPY operations
	opStmt, err := sqlxTx.Prepare(pq.CopyIn("operations",
		"id", "tx_hash", "operation_type", "operation_xdr",
		"ledger_number", "ledger_created_at",
	))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchInsertCopy", "operations", utils.GetDBErrorType(err))
		return 0, fmt.Errorf("preparing COPY statement for operations: %w", err)
	}
	defer func() { _ = opStmt.Close() }() //nolint:errcheck // COPY statement close errors are non-fatal

	for _, op := range operations {
		_, err = opStmt.Exec(
			op.ID,
			op.TxHash,
			string(op.OperationType),
			op.OperationXDR,
			int(op.LedgerNumber),
			op.LedgerCreatedAt,
		)
		if err != nil {
			m.MetricsService.IncDBQueryError("BatchInsertCopy", "operations", utils.GetDBErrorType(err))
			return 0, fmt.Errorf("COPY exec for operation: %w", err)
		}
	}

	// Flush the COPY buffer for operations
	_, err = opStmt.Exec()
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchInsertCopy", "operations", utils.GetDBErrorType(err))
		return 0, fmt.Errorf("flushing COPY buffer for operations: %w", err)
	}

	// COPY operations_accounts
	if len(stellarAddressesByOpID) > 0 {
		// Build lookup map for ledger_created_at by operation ID
		ledgerCreatedAtByID := make(map[int64]time.Time, len(operations))
		for _, op := range operations {
			ledgerCreatedAtByID[op.ID] = op.LedgerCreatedAt
		}

		oaStmt, err := sqlxTx.Prepare(pq.CopyIn("operations_accounts",
			"operation_id", "account_id", "ledger_created_at",
		))
		if err != nil {
			m.MetricsService.IncDBQueryError("BatchInsertCopy", "operations_accounts", utils.GetDBErrorType(err))
			return 0, fmt.Errorf("preparing COPY statement for operations_accounts: %w", err)
		}
		defer func() { _ = oaStmt.Close() }() //nolint:errcheck // COPY statement close errors are non-fatal

		// Convert sets to slices upfront to avoid channel creation per set
		for opID, addresses := range stellarAddressesByOpID {
			ledgerCreatedAt := ledgerCreatedAtByID[opID]
			addressSlice := addresses.ToSlice()
			for _, address := range addressSlice {
				_, err = oaStmt.Exec(opID, address, ledgerCreatedAt)
				if err != nil {
					m.MetricsService.IncDBQueryError("BatchInsertCopy", "operations_accounts", utils.GetDBErrorType(err))
					return 0, fmt.Errorf("COPY exec for operations_accounts: %w", err)
				}
			}
		}

		// Flush the COPY buffer for operations_accounts
		_, err = oaStmt.Exec()
		if err != nil {
			m.MetricsService.IncDBQueryError("BatchInsertCopy", "operations_accounts", utils.GetDBErrorType(err))
			return 0, fmt.Errorf("flushing COPY buffer for operations_accounts: %w", err)
		}

		m.MetricsService.IncDBQuery("BatchInsertCopy", "operations_accounts")
	}

	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchInsertCopy", "operations", duration)
	m.MetricsService.ObserveDBBatchSize("BatchInsertCopy", "operations", len(operations))
	m.MetricsService.IncDBQuery("BatchInsertCopy", "operations")

	return len(operations), nil
}
