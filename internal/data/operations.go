package data

import (
	"context"
	"fmt"
	"strings"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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

// BatchGetByToIDs gets the operations that are associated with the given transaction ToIDs.
//
// # TOID (Total Order ID) Encoding - SEP-35
//
// Operations and transactions use a 64-bit ID that encodes ordering information:
//
//	| Ledger Sequence (32 bits) | Transaction Order (20 bits) | Operation Index (12 bits) |
//	|---------------------------|-----------------------------|-----------------------------|
//	| bits 63-32                | bits 31-12                  | bits 11-0                   |
//
// Key relationships:
//   - Transaction's to_id has OperationIndex = 0
//   - Operation IDs are 1-indexed within a transaction (1, 2, 3, ...)
//   - Max 4095 operations per transaction (12 bits)
//
// To derive transaction to_id from operation ID:
//
//	tx_to_id = operation_id &^ 0xFFF  (clear lower 12 bits)
//
// To query operations for a transaction:
//
//	WHERE id > tx_to_id AND id < tx_to_id + 4096
//
// The range (tx_to_id, tx_to_id + 4096) captures operation indices 1-4095.
// Using exclusive bounds avoids matching the transaction itself (index 0)
// and the next transaction (index 4096 = 0x1000).
//
// See SEP-35: https://github.com/stellar/stellar-protocol/blob/master/ecosystem/sep-0035.md
func (m *OperationModel) BatchGetByToIDs(ctx context.Context, toIDs []int64, columns string, limit *int32, sortOrder SortOrder) ([]*types.OperationWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "", "id")
	queryBuilder := strings.Builder{}
	// This CTE query implements per-transaction pagination to ensure balanced results.
	// Instead of applying a global LIMIT that could return all operations from just a few
	// transactions, we use ROW_NUMBER() with PARTITION BY to limit results per transaction.
	// This guarantees that each transaction gets at most 'limit' operations, providing
	// more balanced and predictable pagination across multiple transactions.
	// Operations for a tx_to_id are in range (tx_to_id, tx_to_id + 4096) based on TOID encoding.
	query := `
		WITH
			inputs (to_id) AS (
				SELECT * FROM UNNEST($1::bigint[])
			),

			ranked_operations_per_to_id AS (
				SELECT
					o.*,
					i.to_id as tx_to_id,
					ROW_NUMBER() OVER (PARTITION BY i.to_id ORDER BY o.id %s) AS rn
				FROM
					operations o
				JOIN
					inputs i ON o.id > i.to_id AND o.id < i.to_id + 4096
			)
		SELECT %s, id as cursor FROM ranked_operations_per_to_id
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
	err := m.DB.SelectContext(ctx, &operations, query, pq.Array(toIDs))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByToIDs", "operations", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByToIDs", "operations", len(toIDs))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByToIDs", "operations", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting operations by to_ids: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByToIDs", "operations")
	return operations, nil
}

// BatchGetByToID gets operations for a single transaction ToID with pagination support.
// Operations for a transaction are found using TOID range: (tx_to_id, tx_to_id + 4096).
func (m *OperationModel) BatchGetByToID(ctx context.Context, toID int64, columns string, limit *int32, cursor *int64, sortOrder SortOrder) ([]*types.OperationWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "", "id")
	queryBuilder := strings.Builder{}
	// Operations for a tx_to_id are in range (tx_to_id, tx_to_id + 4096) based on TOID encoding.
	queryBuilder.WriteString(fmt.Sprintf(`SELECT %s, id as cursor FROM operations WHERE id > $1 AND id < $1 + 4096`, columns))

	args := []interface{}{toID}
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
	m.MetricsService.ObserveDBQueryDuration("BatchGetByToID", "operations", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByToID", "operations", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting paginated operations by to_id: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByToID", "operations")
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
func (m *OperationModel) BatchGetByStateChangeIDs(ctx context.Context, scToIDs []int64, scOpIDs []int64, scOrders []int64, columns string) ([]*types.OperationWithStateChangeID, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "operations", "id")

	// Build tuples for the IN clause. Since (to_id, operation_id, state_change_order) is the primary key of state_changes,
	// it will be faster to search on this tuple.
	tuples := make([]string, len(scOrders))
	for i := range scOrders {
		tuples[i] = fmt.Sprintf("(%d, %d, %d)", scToIDs[i], scOpIDs[i], scOrders[i])
	}

	query := fmt.Sprintf(`
		SELECT %s, CONCAT(state_changes.to_id, '-', state_changes.operation_id, '-', state_changes.state_change_order) AS state_change_id
		FROM operations
		INNER JOIN state_changes ON operations.id = state_changes.operation_id
		WHERE (state_changes.to_id, state_changes.operation_id, state_changes.state_change_order) IN (%s)
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
	operationTypes := make([]string, len(operations))
	operationXDRs := make([]string, len(operations))
	resultCodes := make([]string, len(operations))
	successfulFlags := make([]bool, len(operations))
	ledgerNumbers := make([]uint32, len(operations))
	ledgerCreatedAts := make([]time.Time, len(operations))

	for i, op := range operations {
		ids[i] = op.ID
		operationTypes[i] = string(op.OperationType)
		operationXDRs[i] = op.OperationXDR
		resultCodes[i] = op.ResultCode
		successfulFlags[i] = op.Successful
		ledgerNumbers[i] = op.LedgerNumber
		ledgerCreatedAts[i] = op.LedgerCreatedAt
	}

	// 2. Flatten the stellarAddressesByOpID into parallel slices, converting to BYTEA
	var opIDs []int64
	var stellarAddressBytes [][]byte
	for opID, addresses := range stellarAddressesByOpID {
		for address := range addresses.Iter() {
			opIDs = append(opIDs, opID)
			addrBytesValue, err := types.AddressBytea(address).Value()
			if err != nil {
				return nil, fmt.Errorf("converting address %s to bytes: %w", address, err)
			}
			addrBytes, ok := addrBytesValue.([]byte)
			if !ok || addrBytes == nil {
				return nil, fmt.Errorf("converting address %s to bytes: unexpected value %T", address, addrBytesValue)
			}
			stellarAddressBytes = append(stellarAddressBytes, addrBytes)
		}
	}

	// Insert operations and operations_accounts links.
	const insertQuery = `
	WITH
	-- Insert operations
	inserted_operations AS (
		INSERT INTO operations
			(id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		SELECT
			o.id, o.operation_type, o.operation_xdr, o.result_code, o.successful, o.ledger_number, o.ledger_created_at
		FROM (
			SELECT
				UNNEST($1::bigint[]) AS id,
				UNNEST($2::text[]) AS operation_type,
				UNNEST($3::text[]) AS operation_xdr,
				UNNEST($4::text[]) AS result_code,
				UNNEST($5::boolean[]) AS successful,
				UNNEST($6::bigint[]) AS ledger_number,
				UNNEST($7::timestamptz[]) AS ledger_created_at
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
				UNNEST($8::bigint[]) AS op_id,
				UNNEST($9::bytea[]) AS account_id
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
		pq.Array(operationTypes),
		pq.Array(operationXDRs),
		pq.Array(resultCodes),
		pq.Array(successfulFlags),
		pq.Array(ledgerNumbers),
		pq.Array(ledgerCreatedAts),
		pq.Array(opIDs),
		pq.Array(stellarAddressBytes),
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

// BatchCopy inserts operations using pgx's binary COPY protocol.
// Uses pgx.Tx for binary format which is faster than lib/pq's text format.
// Uses native pgtype types for optimal performance (see https://github.com/jackc/pgx/issues/763).
//
// IMPORTANT: Unlike BatchInsert which uses ON CONFLICT DO NOTHING, BatchCopy will FAIL
// if any duplicate records exist. The PostgreSQL COPY protocol does not support conflict
// handling. Callers must ensure no duplicates exist before calling this method, or handle
// the unique constraint violation error appropriately.
func (m *OperationModel) BatchCopy(
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
		[]string{"id", "operation_type", "operation_xdr", "result_code", "successful", "ledger_number", "ledger_created_at"},
		pgx.CopyFromSlice(len(operations), func(i int) ([]any, error) {
			op := operations[i]
			return []any{
				pgtype.Int8{Int64: op.ID, Valid: true},
				pgtype.Text{String: string(op.OperationType), Valid: true},
				pgtype.Text{String: op.OperationXDR, Valid: true},
				pgtype.Text{String: op.ResultCode, Valid: true},
				pgtype.Bool{Bool: op.Successful, Valid: true},
				pgtype.Int4{Int32: int32(op.LedgerNumber), Valid: true},
				pgtype.Timestamptz{Time: op.LedgerCreatedAt, Valid: true},
			}, nil
		}),
	)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchCopy", "operations", utils.GetDBErrorType(err))
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
				var addrBytes any
				addrBytes, err = types.AddressBytea(addr).Value()
				if err != nil {
					return 0, fmt.Errorf("converting address %s to bytes: %w", addr, err)
				}
				oaRows = append(oaRows, []any{opIDPgtype, addrBytes})
			}
		}

		_, err = pgxTx.CopyFrom(
			ctx,
			pgx.Identifier{"operations_accounts"},
			[]string{"operation_id", "account_id"},
			pgx.CopyFromRows(oaRows),
		)
		if err != nil {
			m.MetricsService.IncDBQueryError("BatchCopy", "operations_accounts", utils.GetDBErrorType(err))
			return 0, fmt.Errorf("pgx CopyFrom operations_accounts: %w", err)
		}

		m.MetricsService.IncDBQuery("BatchCopy", "operations_accounts")
	}

	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchCopy", "operations", duration)
	m.MetricsService.ObserveDBBatchSize("BatchCopy", "operations", len(operations))
	m.MetricsService.IncDBQuery("BatchCopy", "operations")

	return len(operations), nil
}
