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

type OperationModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

func (m *OperationModel) GetByID(ctx context.Context, id int64, columns string) (*types.Operation, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "", "id", "ledger_created_at")
	query := fmt.Sprintf(`SELECT %s FROM operations WHERE id = $1`, columns)
	start := time.Now()
	operation, err := db.QueryOne[types.Operation](ctx, m.DB, query, id)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByID", "operations").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByID", "operations").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByID", "operations", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting operation by id: %w", err)
	}
	return &operation, nil
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
func (m *OperationModel) BatchGetByToIDs(ctx context.Context, toIDs []int64, ledgerCreatedAts []time.Time, columns string, limit *int32, sortOrder SortOrder) ([]*types.OperationWithCursor, error) {
	if len(toIDs) != len(ledgerCreatedAts) {
		return nil, fmt.Errorf("toIDs and ledgerCreatedAts must be parallel arrays of equal length, got %d and %d", len(toIDs), len(ledgerCreatedAts))
	}
	columns = prepareColumnsWithID(columns, types.Operation{}, "o", "id", "ledger_created_at")

	// Operations for a tx_to_id are in range (tx_to_id, tx_to_id + 4096) based on TOID encoding
	// (see the package doc above). Each key carries its parent transaction's ledger_created_at,
	// so the LATERAL probe pins both the TOID band and the partition column, giving per-key
	// runtime chunk exclusion instead of a Parallel Append across the whole hypertable. The
	// ORDER BY + LIMIT live inside the LATERAL (a per-key top-N), replacing the old
	// ROW_NUMBER()-over-everything CTE with an equivalent per-transaction cap; a subquery
	// containing LIMIT cannot be flattened into a join, so the planner must run this as one
	// per-key probe (see BatchGetByAccountAddress). Only the requested columns are selected
	// inside the LATERAL, avoiding decompressing full rows before projection — so
	// cursor_ledger_created_at reads k.ledger_created_at (from UNNEST) rather than
	// o.ledger_created_at, which the LATERAL may not have projected; the two are identical for
	// any row that survives the WHERE match. o.id is safe to reference directly since
	// prepareColumnsWithID always forces "id" into the projection.
	//
	// OFFSET 0 is an optimization fence: it keeps the chunk-selecting scan (which gets runtime
	// chunk exclusion from the ledger_created_at equality) planned separately from the ORDER
	// BY/LIMIT, which would otherwise force a Merge Append probing every chunk's sparse index —
	// O(chunk count) instead of O(1) with retention. After the fence, ORDER BY reads the
	// subquery's bare output column name ("id"), not "o.id" — there is no "o" in scope at that
	// level, only the fenced derived table.
	args := []interface{}{toIDs, ledgerCreatedAts}
	var queryBuilder strings.Builder
	fmt.Fprintf(&queryBuilder, `
		SELECT %s, k.ledger_created_at as cursor_ledger_created_at, o.id as cursor_id
		FROM UNNEST($1::bigint[], $2::timestamptz[]) AS k(to_id, ledger_created_at)
		CROSS JOIN LATERAL (
			SELECT * FROM (
				SELECT %s FROM operations o
				WHERE o.id > k.to_id AND o.id < k.to_id + 4096 AND o.ledger_created_at = k.ledger_created_at
				OFFSET 0
			) sub
			ORDER BY id %s`, columns, columns, sortOrder)
	if limit != nil {
		queryBuilder.WriteString(" LIMIT $3")
		args = append(args, *limit)
	}
	queryBuilder.WriteString(`
		) o
	`)

	query := queryBuilder.String()
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS operations ORDER BY operations.cursor_ledger_created_at ASC, operations.cursor_id ASC`, query)
	}

	start := time.Now()
	operations, err := db.QueryManyPtrs[types.OperationWithCursor](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByToIDs", "operations").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchGetByToIDs", "operations").Observe(float64(len(toIDs)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByToIDs", "operations").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByToIDs", "operations", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting operations by to_ids: %w", err)
	}
	return operations, nil
}

// BatchGetByToID gets operations for a single transaction ToID with pagination support.
// Operations for a transaction are found using TOID range: (tx_to_id, tx_to_id + 4096), pinned
// to the parent transaction's ledger_created_at for partition-column chunk exclusion.
func (m *OperationModel) BatchGetByToID(ctx context.Context, toID int64, ledgerCreatedAt time.Time, columns string, limit *int32, cursor *int64, sortOrder SortOrder) ([]*types.OperationWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "", "id", "ledger_created_at")
	queryBuilder := strings.Builder{}
	// Operations for a tx_to_id are in range (tx_to_id, tx_to_id + 4096) based on TOID encoding.
	fmt.Fprintf(&queryBuilder, `SELECT %s, ledger_created_at as cursor_ledger_created_at, id as cursor_id FROM operations WHERE id > $1 AND id < $1 + 4096 AND ledger_created_at = $2`, columns)

	args := []interface{}{toID, ledgerCreatedAt}
	argIndex := 3

	if cursor != nil {
		if sortOrder == DESC {
			fmt.Fprintf(&queryBuilder, " AND id < $%d", argIndex)
		} else {
			fmt.Fprintf(&queryBuilder, " AND id > $%d", argIndex)
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
		fmt.Fprintf(&queryBuilder, " LIMIT $%d", argIndex)
		args = append(args, *limit)
	}

	query := queryBuilder.String()
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS operations ORDER BY operations.cursor_id ASC`, query)
	}

	start := time.Now()
	operations, err := db.QueryManyPtrs[types.OperationWithCursor](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByToID", "operations").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByToID", "operations").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByToID", "operations", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting paginated operations by to_id: %w", err)
	}
	return operations, nil
}

// BatchGetAccountOperationsByToIDs returns, for each (toID, ledgerCreatedAt) pair, all operations
// in that transaction involving accountAddress. ledgerCreatedAt is the parent transaction's ledger
// time (== the operation's ledger time); pinning the partition column triggers primary chunk
// exclusion. operations has no account column, so scope through operations_accounts
// (segmentby=account_id) via the TOID band, then a PK LATERAL into operations.
//
// Both LATERAL subqueries carry a LIMIT on purpose: a subquery containing LIMIT cannot be
// flattened into a join, so the planner must execute each one as a per-pair index probe.
// Flattened, it could instead scan the account's entire operations_accounts history (or, for
// operations — which has no segmentby column — whole-network data) and hash-join the pairs,
// which on compressed chunks means decompressing every batch instead of exactly one per probe.
// The LIMIT values are invariants, not tuning: a TOID reserves 12 bits for the operation
// index, so one transaction holds at most 4095 operations (LIMIT 4096); the operations probe
// pins the complete primary key, so at most one row can match (LIMIT 1).
func (m *OperationModel) BatchGetAccountOperationsByToIDs(ctx context.Context, accountAddress string, toIDs []int64, ledgerCreatedAts []time.Time, columns string) ([]*types.Operation, error) {
	if len(toIDs) != len(ledgerCreatedAts) {
		return nil, fmt.Errorf("toIDs and ledgerCreatedAts must be parallel arrays of equal length, got %d and %d", len(toIDs), len(ledgerCreatedAts))
	}
	columns = prepareColumnsWithID(columns, types.Operation{}, "o", "id", "ledger_created_at")
	query := fmt.Sprintf(`
		SELECT %s
		FROM UNNEST($2::bigint[], $3::timestamptz[]) AS i(to_id, ledger_created_at)
		CROSS JOIN LATERAL (
			SELECT operation_id, ledger_created_at
			FROM operations_accounts oa
			WHERE oa.ledger_created_at = i.ledger_created_at
			  AND oa.operation_id > i.to_id AND oa.operation_id < i.to_id + 4096
			  AND oa.account_id = $1
			LIMIT 4096
		) oa
		CROSS JOIN LATERAL (
			SELECT * FROM operations o
			WHERE o.id = oa.operation_id AND o.ledger_created_at = oa.ledger_created_at
			LIMIT 1
		) o
		ORDER BY o.ledger_created_at DESC, o.id DESC
	`, columns)

	start := time.Now()
	operations, err := db.QueryManyPtrs[types.Operation](ctx, m.DB, query, types.AddressBytea(accountAddress), toIDs, ledgerCreatedAts)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetAccountOperationsByToIDs", "operations").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchGetAccountOperationsByToIDs", "operations").Observe(float64(len(toIDs)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetAccountOperationsByToIDs", "operations").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetAccountOperationsByToIDs", "operations", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting account operations by to_ids: %w", err)
	}
	return operations, nil
}

// BatchGetByAccountAddress gets the operations that are associated with a single account address.
// Uses a MATERIALIZED CTE + LATERAL join pattern to allow TimescaleDB ChunkAppend optimization
// on the operations_accounts hypertable by ordering on ledger_created_at first.
func (m *OperationModel) BatchGetByAccountAddress(ctx context.Context, accountAddress string, columns string, limit *int32, cursor *types.CompositeCursor, orderBy SortOrder, timeRange *TimeRange) ([]*types.OperationWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Operation{}, "o", "id", "ledger_created_at")

	var queryBuilder strings.Builder
	args := []interface{}{types.AddressBytea(accountAddress)}
	argIndex := 2

	// MATERIALIZED CTE scans operations_accounts with ledger_created_at leading the ORDER BY,
	// enabling TimescaleDB ChunkAppend on the hypertable.
	queryBuilder.WriteString(`
		WITH account_ops AS MATERIALIZED (
			SELECT operation_id, ledger_created_at
			FROM operations_accounts
			WHERE account_id = $1`)

	// Time range filter: enables TimescaleDB chunk pruning at the earliest query stage
	args, argIndex = appendTimeRangeConditions(&queryBuilder, "ledger_created_at", timeRange, args, argIndex)

	// Decomposed cursor pagination: expands ROW() tuple comparison into OR clauses so
	// TimescaleDB ColumnarScan can push filters into vectorized batch processing.
	if cursor != nil {
		clause, cursorArgs, nextIdx := buildDecomposedCursorCondition([]CursorColumn{
			{Name: "ledger_created_at", Value: cursor.LedgerCreatedAt},
			{Name: "operation_id", Value: cursor.ID},
		}, orderBy, argIndex)
		queryBuilder.WriteString("\n\t\t\tAND " + clause)
		args = append(args, cursorArgs...)
		argIndex = nextIdx
	}

	if orderBy == DESC {
		queryBuilder.WriteString(`
			ORDER BY ledger_created_at DESC, operation_id DESC`)
	} else {
		queryBuilder.WriteString(`
			ORDER BY ledger_created_at ASC, operation_id ASC`)
	}

	if limit != nil {
		fmt.Fprintf(&queryBuilder, ` LIMIT $%d`, argIndex)
		args = append(args, *limit)
	}

	// Close CTE and LATERAL join to fetch full operation rows. The LIMIT 1 in the lateral
	// is load-bearing: the complete primary key (id, ledger_created_at) is pinned, so at
	// most one row can match, and a subquery containing LIMIT cannot be flattened into a
	// join — the planner must keep this a per-row PK probe. Flattened, it could scan
	// operations (which has no segmentby column) and hash-join, which on compressed
	// chunks means decompressing whole-network data instead of one batch per probe.
	fmt.Fprintf(&queryBuilder, `
		)
		SELECT %s, o.ledger_created_at as cursor_ledger_created_at, o.id as cursor_id
		FROM account_ops ao
		LEFT JOIN LATERAL (SELECT * FROM operations o WHERE o.id = ao.operation_id AND o.ledger_created_at = ao.ledger_created_at LIMIT 1) o ON true
		WHERE o.id IS NOT NULL`, columns)

	if orderBy == DESC {
		queryBuilder.WriteString(`
		ORDER BY o.ledger_created_at DESC, o.id DESC`)
	} else {
		queryBuilder.WriteString(`
		ORDER BY o.ledger_created_at ASC, o.id ASC`)
	}

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order
	if orderBy == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS operations ORDER BY operations.cursor_ledger_created_at ASC, operations.cursor_id ASC`, query)
	}

	start := time.Now()
	operations, err := db.QueryManyPtrs[types.OperationWithCursor](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByAccountAddress", "operations").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByAccountAddress", "operations").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByAccountAddress", "operations", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting operations by account address: %w", err)
	}
	return operations, nil
}

// BatchGetByStateChangeIDs gets the operations that are associated with the given state change IDs.
// Callers supply each key's parent ledger_created_at (the state change's ledger time, same ledger
// as its operation) so the LATERAL can pin the complete operations primary key (id,
// ledger_created_at) instead of joining through state_changes and hash-joining a full IN-list.
func (m *OperationModel) BatchGetByStateChangeIDs(ctx context.Context, scToIDs []int64, scOpIDs []int64, stateChangeIDs []int64, ledgerCreatedAts []time.Time, columns string) ([]*types.OperationWithStateChangeID, error) {
	if len(scOpIDs) != len(ledgerCreatedAts) {
		return nil, fmt.Errorf("scOpIDs and ledgerCreatedAts must be parallel arrays of equal length, got %d and %d", len(scOpIDs), len(ledgerCreatedAts))
	}
	columns = prepareColumnsWithID(columns, types.Operation{}, "o", "id", "ledger_created_at")

	// ORDER BY reads k.ledger_created_at (from UNNEST), not o.ledger_created_at: the LATERAL
	// only projects the caller-requested columns, which may not include ledger_created_at, but
	// k.ledger_created_at is always in scope and identical to o.ledger_created_at for any row
	// that survives the WHERE match.
	query := fmt.Sprintf(`
		SELECT %s, CONCAT(k.to_id, '-', k.operation_id, '-', k.state_change_id) AS state_change_id
		FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::timestamptz[]) AS k(to_id, operation_id, state_change_id, ledger_created_at)
		CROSS JOIN LATERAL (
			SELECT %s FROM operations o
			WHERE o.id = k.operation_id AND o.ledger_created_at = k.ledger_created_at
			LIMIT 1
		) o
		ORDER BY k.ledger_created_at DESC
	`, columns, columns)

	start := time.Now()
	operationsWithStateChanges, err := db.QueryManyPtrs[types.OperationWithStateChangeID](ctx, m.DB, query, scToIDs, scOpIDs, stateChangeIDs, ledgerCreatedAts)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByStateChangeIDs", "operations").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchGetByStateChangeIDs", "operations").Observe(float64(len(stateChangeIDs)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByStateChangeIDs", "operations").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByStateChangeIDs", "operations", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting operations by state change IDs: %w", err)
	}
	return operationsWithStateChanges, nil
}

// BatchCopy inserts operations using pgx's binary COPY protocol.
// Uses pgx.Tx for binary format which is faster than lib/pq's text format.
// Uses native pgtype types for optimal performance (see https://github.com/jackc/pgx/issues/763).
//
// IMPORTANT: BatchCopy will FAIL if any duplicate records exist. The PostgreSQL COPY
// protocol does not support conflict handling. Callers must ensure no duplicates exist
// before calling this method, or handle the unique constraint violation error appropriately.
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
				[]byte(op.OperationXDR),
				pgtype.Text{String: op.ResultCode, Valid: true},
				pgtype.Bool{Bool: op.Successful, Valid: true},
				pgtype.Int4{Int32: int32(op.LedgerNumber), Valid: true},
				pgtype.Timestamptz{Time: op.LedgerCreatedAt, Valid: true},
			}, nil
		}),
	)
	if err != nil {
		duration := time.Since(start).Seconds()
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "operations").Observe(duration)
		m.Metrics.BatchSize.WithLabelValues("BatchCopy", "operations").Observe(float64(len(operations)))
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "operations").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "operations", utils.GetDBErrorType(err)).Inc()
		return 0, fmt.Errorf("pgx CopyFrom operations: %w", err)
	}
	if int(copyCount) != len(operations) {
		duration := time.Since(start).Seconds()
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "operations").Observe(duration)
		m.Metrics.BatchSize.WithLabelValues("BatchCopy", "operations").Observe(float64(len(operations)))
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "operations").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "operations", "row_count_mismatch").Inc()
		return 0, fmt.Errorf("expected %d rows copied, got %d", len(operations), copyCount)
	}

	// COPY operations_accounts using pgx binary format with native pgtype types. Upstream participants handling ensures that
	// account address is not NULL here.
	if len(stellarAddressesByOpID) > 0 {
		// Build OpID -> LedgerCreatedAt lookup from operations
		ledgerCreatedAtByOpID := make(map[int64]time.Time, len(operations))
		for _, op := range operations {
			ledgerCreatedAtByOpID[op.ID] = op.LedgerCreatedAt
		}

		var oaRows [][]any
		for opID, addresses := range stellarAddressesByOpID {
			ledgerCreatedAt := ledgerCreatedAtByOpID[opID]
			ledgerCreatedAtPgtype := pgtype.Timestamptz{Time: ledgerCreatedAt, Valid: true}
			opIDPgtype := pgtype.Int8{Int64: opID, Valid: true}
			for _, addr := range addresses.ToSlice() {
				addrBytes, addrErr := types.AddressBytea(addr).Value()
				if addrErr != nil {
					return 0, fmt.Errorf("converting address %s to bytes: %w", addr, addrErr)
				}
				oaRows = append(oaRows, []any{
					ledgerCreatedAtPgtype,
					opIDPgtype,
					addrBytes,
				})
			}
		}

		_, err = pgxTx.CopyFrom(
			ctx,
			pgx.Identifier{"operations_accounts"},
			[]string{"ledger_created_at", "operation_id", "account_id"},
			pgx.CopyFromRows(oaRows),
		)
		if err != nil {
			duration := time.Since(start).Seconds()
			m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "operations").Observe(duration)
			m.Metrics.BatchSize.WithLabelValues("BatchCopy", "operations").Observe(float64(len(operations)))
			m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "operations").Inc()
			m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "operations_accounts", utils.GetDBErrorType(err)).Inc()
			return 0, fmt.Errorf("pgx CopyFrom operations_accounts: %w", err)
		}

		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "operations_accounts").Inc()
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "operations").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchCopy", "operations").Observe(float64(len(operations)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "operations").Inc()

	return len(operations), nil
}
