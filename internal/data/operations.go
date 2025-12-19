package data

import (
	"context"
	"fmt"
	"strings"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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
	operations []*types.Operation,
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
				UNNEST($4::text[]) AS operation_xdr,
				UNNEST($5::bigint[]) AS ledger_number,
				UNNEST($6::timestamptz[]) AS ledger_created_at
		) o
		ON CONFLICT (id) DO NOTHING
		RETURNING id
	),

	-- Insert operations_accounts links
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
		ON CONFLICT DO NOTHING
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

// BatchCopy inserts operations using PostgreSQL COPY protocol.
// This variant accepts pointer slices to avoid unnecessary memory copies.
// Optimized for backfill operations with pre-allocated participant lists.
func (m *OperationModel) BatchCopy(
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
		m.MetricsService.IncDBQueryError("BatchCopy", "operations", utils.GetDBErrorType(err))
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
			m.MetricsService.IncDBQueryError("BatchCopy", "operations", utils.GetDBErrorType(err))
			return 0, fmt.Errorf("COPY exec for operation: %w", err)
		}
	}

	// Flush the COPY buffer for operations
	_, err = opStmt.Exec()
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchCopy", "operations", utils.GetDBErrorType(err))
		return 0, fmt.Errorf("flushing COPY buffer for operations: %w", err)
	}

	// COPY operations_accounts
	if len(stellarAddressesByOpID) > 0 {
		oaStmt, err := sqlxTx.Prepare(pq.CopyIn("operations_accounts",
			"operation_id", "account_id",
		))
		if err != nil {
			m.MetricsService.IncDBQueryError("BatchCopy", "operations_accounts", utils.GetDBErrorType(err))
			return 0, fmt.Errorf("preparing COPY statement for operations_accounts: %w", err)
		}
		defer func() { _ = oaStmt.Close() }() //nolint:errcheck // COPY statement close errors are non-fatal

		// Convert sets to slices upfront to avoid channel creation per set
		for opID, addresses := range stellarAddressesByOpID {
			addressSlice := addresses.ToSlice()
			for _, address := range addressSlice {
				_, err = oaStmt.Exec(opID, address)
				if err != nil {
					m.MetricsService.IncDBQueryError("BatchCopy", "operations_accounts", utils.GetDBErrorType(err))
					return 0, fmt.Errorf("COPY exec for operations_accounts: %w", err)
				}
			}
		}

		// Flush the COPY buffer for operations_accounts
		_, err = oaStmt.Exec()
		if err != nil {
			m.MetricsService.IncDBQueryError("BatchCopy", "operations_accounts", utils.GetDBErrorType(err))
			return 0, fmt.Errorf("flushing COPY buffer for operations_accounts: %w", err)
		}

		m.MetricsService.IncDBQuery("BatchCopy", "operations_accounts")
	}

	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchCopy", "operations", duration)
	m.MetricsService.ObserveDBBatchSize("BatchCopy", "operations", len(operations))
	m.MetricsService.IncDBQuery("BatchCopy", "operations")

	return len(operations), nil
}

// BatchCopyPgx inserts operations using pgx's binary COPY protocol.
// Uses pgx.Tx for binary format which is faster than lib/pq's text format.
// Uses native pgtype types for optimal performance (see https://github.com/jackc/pgx/issues/763).
func (m *OperationModel) BatchCopyPgx(
	ctx context.Context,
	pgxTx pgx.Tx,
	operations []*types.Operation,
	stellarAddressesByOpID map[int64]set.Set[string],
) (int, error) {
	if len(operations) == 0 {
		return 0, nil
	}

	start := time.Now()

	// COPY operations using pgx binary format with native pgtype types
	copyCount, err := pgxTx.CopyFrom(
		ctx,
		pgx.Identifier{"operations"},
		[]string{"id", "tx_hash", "operation_type", "operation_xdr", "ledger_number", "ledger_created_at"},
		pgx.CopyFromSlice(len(operations), func(i int) ([]any, error) {
			op := operations[i]
			return []any{
				pgtype.Int8{Int64: op.ID, Valid: true},
				pgtype.Text{String: op.TxHash, Valid: true},
				pgtype.Text{String: string(op.OperationType), Valid: true},
				pgtype.Text{String: op.OperationXDR, Valid: true},
				pgtype.Int4{Int32: int32(op.LedgerNumber), Valid: true},
				pgtype.Timestamptz{Time: op.LedgerCreatedAt, Valid: true},
			}, nil
		}),
	)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchCopyPgx", "operations", utils.GetDBErrorType(err))
		return 0, fmt.Errorf("pgx CopyFrom operations: %w", err)
	}
	if int(copyCount) != len(operations) {
		return 0, fmt.Errorf("expected %d rows copied, got %d", len(operations), copyCount)
	}

	// COPY operations_accounts using pgx binary format with native pgtype types
	if len(stellarAddressesByOpID) > 0 {
		var oaRows [][]any
		for opID, addresses := range stellarAddressesByOpID {
			opIDPgtype := pgtype.Int8{Int64: opID, Valid: true}
			for _, addr := range addresses.ToSlice() {
				oaRows = append(oaRows, []any{opIDPgtype, pgtype.Text{String: addr, Valid: true}})
			}
		}

		_, err = pgxTx.CopyFrom(
			ctx,
			pgx.Identifier{"operations_accounts"},
			[]string{"operation_id", "account_id"},
			pgx.CopyFromRows(oaRows),
		)
		if err != nil {
			m.MetricsService.IncDBQueryError("BatchCopyPgx", "operations_accounts", utils.GetDBErrorType(err))
			return 0, fmt.Errorf("pgx CopyFrom operations_accounts: %w", err)
		}

		m.MetricsService.IncDBQuery("BatchCopyPgx", "operations_accounts")
	}

	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchCopyPgx", "operations", duration)
	m.MetricsService.ObserveDBBatchSize("BatchCopyPgx", "operations", len(operations))
	m.MetricsService.IncDBQuery("BatchCopyPgx", "operations")

	return len(operations), nil
}
