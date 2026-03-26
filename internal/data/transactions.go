package data

import (
	"context"
	"fmt"
	"strings"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type TransactionModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

func (m *TransactionModel) GetByHash(ctx context.Context, hash string, columns string) (*types.Transaction, error) {
	columns = prepareColumnsWithID(columns, types.Transaction{}, "", "to_id")
	query := fmt.Sprintf(`SELECT %s FROM transactions WHERE hash = $1`, columns)
	start := time.Now()
	hashBytea := types.HashBytea(hash)
	transaction, err := db.QueryOne[types.Transaction](ctx, m.DB, query, hashBytea)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByHash", "transactions").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByHash", "transactions").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByHash", "transactions", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting transaction %s: %w", hash, err)
	}
	return &transaction, nil
}

func (m *TransactionModel) GetAll(ctx context.Context, columns string, limit *int32, cursor *types.CompositeCursor, sortOrder SortOrder) ([]*types.TransactionWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Transaction{}, "", "to_id")
	queryBuilder := strings.Builder{}
	var args []interface{}
	argIndex := 1

	queryBuilder.WriteString(fmt.Sprintf(`SELECT %s, ledger_created_at as cursor_ledger_created_at, to_id as cursor_id FROM transactions`, columns))

	// Decomposed cursor pagination: expands ROW() tuple comparison into OR clauses so
	// TimescaleDB ColumnarScan can push filters into vectorized batch processing.
	if cursor != nil {
		clause, cursorArgs, nextIdx := buildDecomposedCursorCondition([]CursorColumn{
			{Name: "ledger_created_at", Value: cursor.LedgerCreatedAt},
			{Name: "to_id", Value: cursor.ID},
		}, sortOrder, argIndex)
		queryBuilder.WriteString(" WHERE " + clause)
		args = append(args, cursorArgs...)
		argIndex = nextIdx
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY ledger_created_at DESC, to_id DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY ledger_created_at ASC, to_id ASC")
	}

	if limit != nil {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT $%d", argIndex))
		args = append(args, *limit)
	}

	query := queryBuilder.String()
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS transactions ORDER BY transactions.cursor_ledger_created_at ASC, transactions.cursor_id ASC`, query)
	}

	start := time.Now()
	transactions, err := db.QueryManyPtrs[types.TransactionWithCursor](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetAll", "transactions").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetAll", "transactions").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetAll", "transactions", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting transactions: %w", err)
	}
	return transactions, nil
}

// BatchGetByAccountAddress gets the transactions that are associated with a single account address.
// Uses a MATERIALIZED CTE + LATERAL join pattern to allow TimescaleDB ChunkAppend optimization
// on the transactions_accounts hypertable by ordering on ledger_created_at first.
func (m *TransactionModel) BatchGetByAccountAddress(ctx context.Context, accountAddress string, columns string, limit *int32, cursor *types.CompositeCursor, orderBy SortOrder, timeRange *TimeRange) ([]*types.TransactionWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Transaction{}, "t", "to_id")

	var queryBuilder strings.Builder
	args := []interface{}{types.AddressBytea(accountAddress)}
	argIndex := 2

	// MATERIALIZED CTE scans transactions_accounts with ledger_created_at leading the ORDER BY,
	// enabling TimescaleDB ChunkAppend on the hypertable.
	queryBuilder.WriteString(`
		WITH account_txns AS MATERIALIZED (
			SELECT tx_to_id, ledger_created_at
			FROM transactions_accounts
			WHERE account_id = $1`)

	// Time range filter: enables TimescaleDB chunk pruning at the earliest query stage
	args, argIndex = appendTimeRangeConditions(&queryBuilder, "ledger_created_at", timeRange, args, argIndex)

	// Decomposed cursor pagination: expands ROW() tuple comparison into OR clauses so
	// TimescaleDB ColumnarScan can push filters into vectorized batch processing.
	if cursor != nil {
		clause, cursorArgs, nextIdx := buildDecomposedCursorCondition([]CursorColumn{
			{Name: "ledger_created_at", Value: cursor.LedgerCreatedAt},
			{Name: "tx_to_id", Value: cursor.ID},
		}, orderBy, argIndex)
		queryBuilder.WriteString("\n\t\t\tAND " + clause)
		args = append(args, cursorArgs...)
		argIndex = nextIdx
	}

	if orderBy == DESC {
		queryBuilder.WriteString(`
			ORDER BY ledger_created_at DESC, tx_to_id DESC`)
	} else {
		queryBuilder.WriteString(`
			ORDER BY ledger_created_at ASC, tx_to_id ASC`)
	}

	if limit != nil {
		queryBuilder.WriteString(fmt.Sprintf(` LIMIT $%d`, argIndex))
		args = append(args, *limit)
	}

	// Close CTE and LATERAL join to fetch full transaction rows
	queryBuilder.WriteString(fmt.Sprintf(`
		)
		SELECT %s, t.ledger_created_at as cursor_ledger_created_at, t.to_id as cursor_id
		FROM account_txns ta
		LEFT JOIN LATERAL (SELECT * FROM transactions t WHERE t.to_id = ta.tx_to_id AND t.ledger_created_at = ta.ledger_created_at LIMIT 1) t ON true
		WHERE t.to_id IS NOT NULL`, columns))

	if orderBy == DESC {
		queryBuilder.WriteString(`
		ORDER BY t.ledger_created_at DESC, t.to_id DESC`)
	} else {
		queryBuilder.WriteString(`
		ORDER BY t.ledger_created_at ASC, t.to_id ASC`)
	}

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order
	if orderBy == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS transactions ORDER BY transactions.cursor_ledger_created_at ASC, transactions.cursor_id ASC`, query)
	}

	start := time.Now()
	transactions, err := db.QueryManyPtrs[types.TransactionWithCursor](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByAccountAddress", "transactions").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByAccountAddress", "transactions").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByAccountAddress", "transactions", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting transactions by account address: %w", err)
	}
	return transactions, nil
}

// BatchGetByOperationIDs gets the transactions that are associated with the given operation IDs.
//
// Uses TOID bit masking to derive tx_to_id from operation ID:
//
//	tx_to_id = operation_id &^ 0xFFF
//
// This works because TOID encodes (ledger, tx_order, op_index) where:
//   - Lower 12 bits = operation index (1-4095, or 0 for transaction itself)
//   - Clearing these bits yields the transaction's to_id
//
// See SEP-35: https://github.com/stellar/stellar-protocol/blob/master/ecosystem/sep-0035.md
func (m *TransactionModel) BatchGetByOperationIDs(ctx context.Context, operationIDs []int64, columns string) ([]*types.TransactionWithOperationID, error) {
	columns = prepareColumnsWithID(columns, types.Transaction{}, "transactions", "to_id")
	// Join operations to transactions using TOID encoding:
	// An operation ID's lower 12 bits encode the operation index within the transaction.
	// Masking these bits (id &~ 0xFFF) gives the transaction's to_id.
	query := fmt.Sprintf(`
		SELECT %s, o.id as operation_id
		FROM operations o
		INNER JOIN transactions
		ON (o.id & (~x'FFF'::bigint)) = transactions.to_id
		WHERE o.id = ANY($1)`, columns)
	start := time.Now()
	// QueryManyPtrs uses RowToAddrOfStructByNameLax: struct may have more fields than selected columns (dynamic `columns` param)
	transactions, err := db.QueryManyPtrs[types.TransactionWithOperationID](ctx, m.DB, query, operationIDs)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByOperationIDs", "transactions").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchGetByOperationIDs", "transactions").Observe(float64(len(operationIDs)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByOperationIDs", "transactions").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByOperationIDs", "transactions", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting transactions by operation IDs: %w", err)
	}
	return transactions, nil
}

// BatchGetByStateChangeIDs gets the transactions that are associated with the given state changes
func (m *TransactionModel) BatchGetByStateChangeIDs(ctx context.Context, scToIDs []int64, scOpIDs []int64, scOrders []int64, columns string) ([]*types.TransactionWithStateChangeID, error) {
	columns = prepareColumnsWithID(columns, types.Transaction{}, "transactions", "to_id")

	// Build tuples for the IN clause. Since (to_id, operation_id, state_change_id) is the primary key of state_changes,
	// it will be faster to search on this tuple.
	tuples := make([]string, len(scOrders))
	for i := range scOrders {
		tuples[i] = fmt.Sprintf("(%d, %d, %d)", scToIDs[i], scOpIDs[i], scOrders[i])
	}

	query := fmt.Sprintf(`
		SELECT %s, CONCAT(sc.to_id, '-', sc.operation_id, '-', sc.state_change_id) as state_change_id
		FROM transactions
		INNER JOIN state_changes sc ON transactions.to_id = sc.to_id
		WHERE (sc.to_id, sc.operation_id, sc.state_change_id) IN (%s)
		`, columns, strings.Join(tuples, ", "))

	start := time.Now()
	transactions, err := db.QueryManyPtrs[types.TransactionWithStateChangeID](ctx, m.DB, query)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByStateChangeIDs", "transactions").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchGetByStateChangeIDs", "transactions").Observe(float64(len(scOrders)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByStateChangeIDs", "transactions").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByStateChangeIDs", "transactions", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting transactions by state change IDs: %w", err)
	}
	return transactions, nil
}

// BatchCopy inserts transactions using pgx's binary COPY protocol.
// Uses pgx.Tx for binary format which is faster than lib/pq's text format.
// Uses native pgtype types for optimal performance (see https://github.com/jackc/pgx/issues/763).
//
// IMPORTANT: BatchCopy will FAIL if any duplicate records exist. The PostgreSQL COPY
// protocol does not support conflict handling. Callers must ensure no duplicates exist
// before calling this method, or handle the unique constraint violation error appropriately.
func (m *TransactionModel) BatchCopy(
	ctx context.Context,
	pgxTx pgx.Tx,
	txs []*types.Transaction,
	stellarAddressesByToID map[int64]set.Set[string],
) (int, error) {
	if len(txs) == 0 {
		return 0, nil
	}

	start := time.Now()

	// COPY transactions using pgx binary format with native pgtype types. Upstream participants handling ensures that
	// account address is not NULL here.
	copyCount, err := pgxTx.CopyFrom(
		ctx,
		pgx.Identifier{"transactions"},
		[]string{"hash", "to_id", "fee_charged", "result_code", "ledger_number", "ledger_created_at", "is_fee_bump"},
		pgx.CopyFromSlice(len(txs), func(i int) ([]any, error) {
			tx := txs[i]
			hashBytes, err := tx.Hash.Value()
			if err != nil {
				return nil, fmt.Errorf("converting hash %s to bytes: %w", tx.Hash, err)
			}
			return []any{
				hashBytes,
				pgtype.Int8{Int64: tx.ToID, Valid: true},
				pgtype.Int8{Int64: tx.FeeCharged, Valid: true},
				pgtype.Text{String: tx.ResultCode, Valid: true},
				pgtype.Int4{Int32: int32(tx.LedgerNumber), Valid: true},
				pgtype.Timestamptz{Time: tx.LedgerCreatedAt, Valid: true},
				pgtype.Bool{Bool: tx.IsFeeBump, Valid: true},
			}, nil
		}),
	)
	if err != nil {
		duration := time.Since(start).Seconds()
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "transactions").Observe(duration)
		m.Metrics.BatchSize.WithLabelValues("BatchCopy", "transactions").Observe(float64(len(txs)))
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "transactions").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "transactions", utils.GetDBErrorType(err)).Inc()
		return 0, fmt.Errorf("pgx CopyFrom transactions: %w", err)
	}
	if int(copyCount) != len(txs) {
		duration := time.Since(start).Seconds()
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "transactions").Observe(duration)
		m.Metrics.BatchSize.WithLabelValues("BatchCopy", "transactions").Observe(float64(len(txs)))
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "transactions").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "transactions", "row_count_mismatch").Inc()
		return 0, fmt.Errorf("expected %d rows copied, got %d", len(txs), copyCount)
	}

	// COPY transactions_accounts using pgx binary format with native pgtype types
	if len(stellarAddressesByToID) > 0 {
		// Build ToID -> LedgerCreatedAt lookup from transactions
		ledgerCreatedAtByToID := make(map[int64]time.Time, len(txs))
		for _, tx := range txs {
			ledgerCreatedAtByToID[tx.ToID] = tx.LedgerCreatedAt
		}

		var taRows [][]any
		for toID, addresses := range stellarAddressesByToID {
			ledgerCreatedAt := ledgerCreatedAtByToID[toID]
			ledgerCreatedAtPgtype := pgtype.Timestamptz{Time: ledgerCreatedAt, Valid: true}
			toIDPgtype := pgtype.Int8{Int64: toID, Valid: true}
			for _, addr := range addresses.ToSlice() {
				addrBytes, addrErr := types.AddressBytea(addr).Value()
				if addrErr != nil {
					return 0, fmt.Errorf("converting address %s to bytes: %w", addr, addrErr)
				}
				taRows = append(taRows, []any{
					ledgerCreatedAtPgtype,
					toIDPgtype,
					addrBytes,
				})
			}
		}

		_, err = pgxTx.CopyFrom(
			ctx,
			pgx.Identifier{"transactions_accounts"},
			[]string{"ledger_created_at", "tx_to_id", "account_id"},
			pgx.CopyFromRows(taRows),
		)
		if err != nil {
			duration := time.Since(start).Seconds()
			m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "transactions").Observe(duration)
			m.Metrics.BatchSize.WithLabelValues("BatchCopy", "transactions").Observe(float64(len(txs)))
			m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "transactions").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "transactions_accounts", utils.GetDBErrorType(err)).Inc()
			return 0, fmt.Errorf("pgx CopyFrom transactions_accounts: %w", err)
		}

		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "transactions_accounts").Inc()
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "transactions").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchCopy", "transactions").Observe(float64(len(txs)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "transactions").Inc()

	return len(txs), nil
}
