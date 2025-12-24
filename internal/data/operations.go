package data

import (
	"context"
	"fmt"
	"log"
	"sort"
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

// BatchGetByTxHashes gets the operations that are associated with the given transaction hashes.
// Uses JOIN with transactions table since tx_hash is not stored in operations table.
func (m *OperationModel) BatchGetByTxHashes(ctx context.Context, txHashes []string, columns string, limit *int32, sortOrder SortOrder) ([]*types.OperationWithCursor, error) {
	// No prefix for outer SELECT since we're selecting from the CTE, not the table directly
	columns = prepareColumnsWithID(columns, types.Operation{}, "", "id")
	queryBuilder := strings.Builder{}
	// This CTE query implements per-transaction pagination to ensure balanced results.
	// Instead of applying a global LIMIT that could return all operations from just a few
	// transactions, we use ROW_NUMBER() with PARTITION BY t.hash to limit results per transaction.
	// Uses JOIN with transactions table via TOID derivation: (o.id & ~4095) = t.to_id
	query := `
		WITH
			inputs (tx_hash) AS (
				SELECT * FROM UNNEST($1::text[])
			),

			ranked_operations_per_tx_hash AS (
				SELECT
					operations.*,
					t.hash as tx_hash,
					ROW_NUMBER() OVER (PARTITION BY t.hash ORDER BY operations.id %s) AS rn
				FROM
					operations
				JOIN
					transactions t ON (operations.id & ~4095) = t.to_id
				JOIN
					inputs i ON t.hash = i.tx_hash
			)
		SELECT %s, id as cursor, tx_hash FROM ranked_operations_per_tx_hash
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
// Uses JOIN with transactions table since tx_hash is not stored in operations table.
func (m *OperationModel) BatchGetByTxHash(ctx context.Context, txHash string, columns string, limit *int32, cursor *int64, sortOrder SortOrder) ([]*types.OperationWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "o", "id")
	queryBuilder := strings.Builder{}
	// JOIN with transactions table via TOID derivation: (o.id & ~4095) = t.to_id
	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, o.id as cursor, t.hash as tx_hash
		FROM operations o
		JOIN transactions t ON (o.id & ~4095) = t.to_id
		WHERE t.hash = $1`, columns))

	args := []interface{}{txHash}
	argIndex := 2

	if cursor != nil {
		if sortOrder == DESC {
			queryBuilder.WriteString(fmt.Sprintf(" AND o.id < $%d", argIndex))
		} else {
			queryBuilder.WriteString(fmt.Sprintf(" AND o.id > $%d", argIndex))
		}
		args = append(args, *cursor)
		argIndex++
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY o.id DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY o.id ASC")
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
// For operation-related state changes, to_id equals the operation ID (when to_id & 4095 != 0).
func (m *OperationModel) BatchGetByStateChangeIDs(ctx context.Context, scToIDs []int64, scOrders []int64, columns string) ([]*types.OperationWithStateChangeID, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "o", "id")

	// Build tuples for the IN clause. Since (to_id, state_change_order) is the primary key of state_changes,
	// it will be faster to search on this tuple.
	tuples := make([]string, len(scOrders))
	for i := range scOrders {
		tuples[i] = fmt.Sprintf("(%d, %d)", scToIDs[i], scOrders[i])
	}

	// JOIN condition: for operation-related state changes, to_id IS the operation ID.
	// We filter only state changes that have operations (to_id & 4095 != 0).
	query := fmt.Sprintf(`
		SELECT %s, CONCAT(sc.to_id, '-', sc.state_change_order) AS state_change_id
		FROM operations o
		INNER JOIN state_changes sc ON o.id = sc.to_id AND (sc.to_id & 4095) != 0
		WHERE (sc.to_id, sc.state_change_order) IN (%s)
		ORDER BY o.ledger_created_at DESC
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
	operationTypes := make([]int16, len(operations))
	operationXDRs := make([]string, len(operations))
	ledgerCreatedAts := make([]time.Time, len(operations))

	// Build opID -> ledgerCreatedAt mapping for operations_accounts
	opIDToLedgerCreatedAt := make(map[int64]time.Time, len(operations))
	for i, op := range operations {
		ids[i] = op.ID
		operationTypes[i] = op.OperationType.ToInt16()
		operationXDRs[i] = op.OperationXDR
		ledgerCreatedAts[i] = op.LedgerCreatedAt
		opIDToLedgerCreatedAt[op.ID] = op.LedgerCreatedAt
	}

	// 2. Flatten the stellarAddressesByOpID into parallel slices
	var opIDs []int64
	var stellarAddresses [][]byte
	var oaLedgerCreatedAts []time.Time
	for opID, addresses := range stellarAddressesByOpID {
		for address := range addresses.Iter() {
			opIDs = append(opIDs, opID)
			stellarAddresses = append(stellarAddresses, bytesFromAddressString(address))
			oaLedgerCreatedAts = append(oaLedgerCreatedAts, opIDToLedgerCreatedAt[opID])
		}
	}

	// Insert operations and operations_accounts links.
	const insertQuery = `
	WITH
	-- Insert operations
	inserted_operations AS (
		INSERT INTO operations
			(id, operation_type, operation_xdr, ledger_created_at)
		SELECT
			o.id, o.operation_type, o.operation_xdr, o.ledger_created_at
		FROM (
			SELECT
				UNNEST($1::bigint[]) AS id,
				UNNEST($2::smallint[]) AS operation_type,
				UNNEST($3::text[]) AS operation_xdr,
				UNNEST($4::timestamptz[]) AS ledger_created_at
		) o
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
				UNNEST($5::bigint[]) AS op_id,
				UNNEST($6::bytea[]) AS account_id,
				UNNEST($7::timestamptz[]) AS ledger_created_at
		) oa
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

// BatchCopy inserts operations using pgx's binary COPY protocol.
// Uses pgx.Tx for binary format which is faster than lib/pq's text format.
// Uses native pgtype types for optimal performance (see https://github.com/jackc/pgx/issues/763).
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

	opCreatedAtByHash := make(map[int64]time.Time)
	for _, op := range operations {
		opCreatedAtByHash[op.ID] = op.LedgerCreatedAt
	}

	// Sort operations by ledger_created_at for batch insertion
	sort.Slice(operations, func(i, j int) bool {
		if operations[i].LedgerCreatedAt.Equal(operations[j].LedgerCreatedAt) {
			return operations[i].ID > operations[j].ID
		}
		return operations[i].LedgerCreatedAt.After(operations[j].LedgerCreatedAt)
	})

	// COPY operations using pgx binary format with native pgtype types
	copyCount, err := pgxTx.CopyFrom(
		ctx,
		pgx.Identifier{"operations"},
		[]string{"id", "operation_type", "operation_xdr", "ledger_created_at"},
		pgx.CopyFromSlice(len(operations), func(i int) ([]any, error) {
			op := operations[i]
			return []any{
				pgtype.Int8{Int64: op.ID, Valid: true},
				pgtype.Int2{Int16: op.OperationType.ToInt16(), Valid: true},
				pgtype.Text{String: op.OperationXDR, Valid: true},
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
		type oaRow struct {
			opID            int64
			addr            []byte
			ledgerCreatedAt time.Time
		}
		var oaRows []oaRow
		for opID, addresses := range stellarAddressesByOpID {
			for addr := range addresses.Iter() {
				if bytesFromAddressString(addr) != nil {
					oaRows = append(oaRows, oaRow{
						opID:            opID,
						addr:            bytesFromAddressString(addr),
						ledgerCreatedAt: opCreatedAtByHash[opID],
					})
				} else {
					continue
				}
			}
		}

		sort.Slice(oaRows, func(i, j int) bool {
			if oaRows[i].ledgerCreatedAt.Equal(oaRows[j].ledgerCreatedAt) {
				return oaRows[i].opID > oaRows[j].opID
			}
			return oaRows[i].ledgerCreatedAt.After(oaRows[j].ledgerCreatedAt)
		})

		_, err = pgxTx.CopyFrom(
			ctx,
			pgx.Identifier{"operations_accounts"},
			[]string{"operation_id", "account_id", "ledger_created_at"},
			pgx.CopyFromSlice(len(oaRows), func(i int) ([]any, error) {
				return []any{
					pgtype.Int8{Int64: oaRows[i].opID, Valid: true},
					oaRows[i].addr,
					pgtype.Timestamptz{Time: oaRows[i].ledgerCreatedAt, Valid: true},
				}, nil
			}),
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
