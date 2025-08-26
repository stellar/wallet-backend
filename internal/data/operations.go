package data

import (
	"context"
	"fmt"
	"strings"
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

func (m *OperationModel) GetByID(ctx context.Context, id int64, columns string) (*types.Operation, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "", "id")
	query := fmt.Sprintf(`SELECT %s FROM operations WHERE id = $1`, columns)
	var operation types.Operation
	start := time.Now()
	err := m.DB.GetContext(ctx, &operation, query, id)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "operations", duration)
	if err != nil {
		return nil, fmt.Errorf("getting operation by id: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "operations")
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
	m.MetricsService.ObserveDBQueryDuration("SELECT", "operations", duration)
	if err != nil {
		return nil, fmt.Errorf("getting operations: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "operations")
	return operations, nil
}

// BatchGetByTxHashes gets the operations that are associated with the given transaction hashes.
func (m *OperationModel) BatchGetByTxHashes(ctx context.Context, txHashes []string, columns string, limit *int32, sortOrder SortOrder) ([]*types.OperationWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "", "id")
	queryBuilder := strings.Builder{}
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
	m.MetricsService.ObserveDBQueryDuration("SELECT", "operations", duration)
	if err != nil {
		return nil, fmt.Errorf("getting operations by tx hashes: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "operations")
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
	m.MetricsService.ObserveDBQueryDuration("SELECT", "operations", duration)
	if err != nil {
		return nil, fmt.Errorf("getting paginated operations by tx hash: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "operations")
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
	m.MetricsService.ObserveDBQueryDuration("SELECT", "operations", duration)
	if err != nil {
		return nil, fmt.Errorf("getting operations by account address: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "operations")
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
