package data

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type StateChangeModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

// StateChangeWriter is the bulk-insert surface external protocol processors use to persist
// history into the state_changes hypertable inside a CAS-guarded transaction.
type StateChangeWriter interface {
	BatchCopy(ctx context.Context, pgxTx pgx.Tx, stateChanges []types.StateChange) (int, error)
}

var _ StateChangeWriter = (*StateChangeModel)(nil)

// BatchGetByAccountAddress gets the state changes that are associated with the given account address.
// Optional filters: txHash, operationID, category, and reason can be used to further filter results.
func (m *StateChangeModel) BatchGetByAccountAddress(ctx context.Context, accountAddress string, txHash *string, operationID *int64, category *string, reason *string, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder, timeRange *TimeRange) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "operation_id", "state_change_id", "account_id", "ledger_created_at")
	var queryBuilder strings.Builder
	args := []interface{}{types.AddressBytea(accountAddress)}
	argIndex := 2

	fmt.Fprintf(&queryBuilder, `
		SELECT %s, ledger_created_at as cursor_ledger_created_at, to_id as cursor_to_id, operation_id as cursor_operation_id, state_change_id as cursor_state_change_id
		FROM state_changes
		WHERE account_id = $1
	`, columns)

	// Time range filter: enables TimescaleDB chunk pruning on the state_changes hypertable
	args, argIndex = appendTimeRangeConditions(&queryBuilder, "ledger_created_at", timeRange, args, argIndex)

	// Add transaction hash filter if provided (uses subquery to find to_id(s) by hash).
	// idx_transactions_hash is non-unique (TimescaleDB can't enforce unique(hash) on a
	// hypertable), so this must tolerate more than one matching row instead of erroring.
	if txHash != nil {
		fmt.Fprintf(&queryBuilder, " AND to_id IN (SELECT to_id FROM transactions WHERE hash = $%d)", argIndex)
		args = append(args, types.HashBytea(*txHash))
		argIndex++
	}

	// Add operation ID filter if provided
	if operationID != nil {
		fmt.Fprintf(&queryBuilder, " AND operation_id = $%d", argIndex)
		args = append(args, *operationID)
		argIndex++
	}

	// Add category filter if provided
	if category != nil {
		fmt.Fprintf(&queryBuilder, " AND state_change_category = $%d", argIndex)
		args = append(args, *category)
		argIndex++
	}

	// Add reason filter if provided
	if reason != nil {
		fmt.Fprintf(&queryBuilder, " AND state_change_reason = $%d", argIndex)
		args = append(args, *reason)
		argIndex++
	}

	// Decomposed cursor pagination: expands ROW() tuple comparison into OR clauses so
	// TimescaleDB ColumnarScan can push filters into vectorized batch processing.
	if cursor != nil {
		clause, cursorArgs, nextIdx := buildDecomposedCursorCondition([]CursorColumn{
			{Name: "ledger_created_at", Value: cursor.LedgerCreatedAt},
			{Name: "to_id", Value: cursor.ToID},
			{Name: "operation_id", Value: cursor.OperationID},
			{Name: "state_change_id", Value: cursor.StateChangeID},
		}, sortOrder, argIndex)
		queryBuilder.WriteString(" AND " + clause)
		args = append(args, cursorArgs...)
		argIndex = nextIdx
	}

	// Add ordering with ledger_created_at as leading column for TimescaleDB ChunkAppend
	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY ledger_created_at DESC, to_id DESC, operation_id DESC, state_change_id DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY ledger_created_at ASC, to_id ASC, operation_id ASC, state_change_id ASC")
	}

	// Add limit using parameterized query
	if limit != nil && *limit > 0 {
		fmt.Fprintf(&queryBuilder, " LIMIT $%d", argIndex)
		args = append(args, *limit)
	}

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order.
	// We use cursor alias columns (e.g., "cursor_ledger_created_at") in ORDER BY to avoid
	// ambiguity since the inner SELECT includes both original columns and cursor aliases.
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY statechanges.cursor_ledger_created_at ASC, statechanges.cursor_to_id ASC, statechanges.cursor_operation_id ASC, statechanges.cursor_state_change_id ASC`, query)
	}

	start := time.Now()
	stateChanges, err := db.QueryManyPtrs[types.StateChangeWithCursor](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByAccountAddress", "state_changes").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByAccountAddress", "state_changes").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByAccountAddress", "state_changes", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting state changes by account address: %w", err)
	}
	return stateChanges, nil
}

func (m *StateChangeModel) GetAll(ctx context.Context, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder, timeRange *TimeRange) ([]*types.StateChangeWithCursor, error) {
	if err := validatePositiveLimit(limit); err != nil {
		return nil, err
	}
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "operation_id", "state_change_id", "account_id", "ledger_created_at")
	var queryBuilder strings.Builder
	var args []interface{}
	argIndex := 1

	// WHERE true gives appendTimeRangeConditions and the cursor condition below a uniform " AND
	// ..." shape to append to, whether or not a time range is present.
	fmt.Fprintf(&queryBuilder, `
		SELECT %s, ledger_created_at as cursor_ledger_created_at, to_id as cursor_to_id, operation_id as cursor_operation_id, state_change_id as cursor_state_change_id
		FROM state_changes
		WHERE true
	`, columns)

	// Time range filter: enables TimescaleDB chunk pruning on the state_changes hypertable. D3's
	// root-connection default window (see buildRootTimeRange) means this is populated even when
	// the client passes neither since nor until.
	args, argIndex = appendTimeRangeConditions(&queryBuilder, "ledger_created_at", timeRange, args, argIndex)

	// Decomposed cursor pagination: expands ROW() tuple comparison into OR clauses so
	// TimescaleDB ColumnarScan can push filters into vectorized batch processing.
	if cursor != nil {
		clause, cursorArgs, nextIdx := buildDecomposedCursorCondition([]CursorColumn{
			{Name: "ledger_created_at", Value: cursor.LedgerCreatedAt},
			{Name: "to_id", Value: cursor.ToID},
			{Name: "operation_id", Value: cursor.OperationID},
			{Name: "state_change_id", Value: cursor.StateChangeID},
		}, sortOrder, argIndex)
		queryBuilder.WriteString(" AND " + clause)
		args = append(args, cursorArgs...)
		argIndex = nextIdx
	}

	// Order with ledger_created_at as leading column for TimescaleDB ChunkAppend
	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY ledger_created_at DESC, to_id DESC, operation_id DESC, state_change_id DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY ledger_created_at ASC, to_id ASC, operation_id ASC, state_change_id ASC")
	}

	if limit != nil {
		fmt.Fprintf(&queryBuilder, " LIMIT $%d", argIndex)
		args = append(args, *limit)
	}

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order.
	// We use cursor alias columns (e.g., "cursor_ledger_created_at") in ORDER BY to avoid
	// ambiguity since the inner SELECT includes both original columns and cursor aliases.
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY statechanges.cursor_ledger_created_at ASC, statechanges.cursor_to_id ASC, statechanges.cursor_operation_id ASC, statechanges.cursor_state_change_id ASC`, query)
	}

	start := time.Now()
	stateChanges, err := db.QueryManyPtrs[types.StateChangeWithCursor](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetAll", "state_changes").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetAll", "state_changes").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetAll", "state_changes", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting all state changes: %w", err)
	}
	return stateChanges, nil
}

// BatchCopy inserts state changes using pgx's binary COPY protocol.
// Uses native pgtype types for optimal performance (see https://github.com/jackc/pgx/issues/763).
//
// Each StateChange must already carry its final, deterministic StateChangeID
// (see types.AssignStateChangeOrdinals) — BatchCopy writes it as-is and never
// generates one.
//
// IMPORTANT: BatchCopy will FAIL if any duplicate records exist. The PostgreSQL COPY
// protocol does not support conflict handling. Callers must ensure no duplicates exist
// before calling this method, or handle the unique constraint violation error
// appropriately. Because state_change_id is now deterministic, this is also the
// idempotency backstop: re-copying an already-committed ledger's rows collides on the
// primary key and fails loudly instead of inserting duplicates.
func (m *StateChangeModel) BatchCopy(
	ctx context.Context,
	pgxTx pgx.Tx,
	stateChanges []types.StateChange,
) (int, error) {
	if len(stateChanges) == 0 {
		return 0, nil
	}

	start := time.Now()

	// COPY state_changes using pgx binary format with native pgtype types
	copyCount, err := pgxTx.CopyFrom(
		ctx,
		pgx.Identifier{"state_changes"},
		[]string{
			"to_id", "state_change_id", "state_change_category", "state_change_reason",
			"ledger_created_at", "ledger_number", "account_id", "operation_id",
			"token_id", "amount", "signer_account_id", "spender_account_id",
			"sponsored_account_id", "sponsor_account_id", "deployer_account_id", "funder_account_id",
			"destination_account_id", "claimable_balance_id", "liquidity_pool_id", "sponsored_data",
			"signer_weight_old", "signer_weight_new", "threshold_old", "threshold_new",
			"trustline_limit_old", "trustline_limit_new", "flags", "key_value",
			"to_muxed_id",
		},
		pgx.CopyFromSlice(len(stateChanges), func(i int) ([]any, error) {
			sc := stateChanges[i]

			// Convert account_id to BYTEA (required field)
			accountBytes, err := sc.AccountID.Value()
			if err != nil {
				return nil, fmt.Errorf("converting account_id: %w", err)
			}

			// Convert nullable account_id fields to BYTEA
			signerBytes, err := pgtypeBytesFromNullAddressBytea(sc.SignerAccountID)
			if err != nil {
				return nil, fmt.Errorf("converting signer_account_id: %w", err)
			}
			spenderBytes, err := pgtypeBytesFromNullAddressBytea(sc.SpenderAccountID)
			if err != nil {
				return nil, fmt.Errorf("converting spender_account_id: %w", err)
			}
			sponsoredBytes, err := pgtypeBytesFromNullAddressBytea(sc.SponsoredAccountID)
			if err != nil {
				return nil, fmt.Errorf("converting sponsored_account_id: %w", err)
			}
			sponsorBytes, err := pgtypeBytesFromNullAddressBytea(sc.SponsorAccountID)
			if err != nil {
				return nil, fmt.Errorf("converting sponsor_account_id: %w", err)
			}
			deployerBytes, err := pgtypeBytesFromNullAddressBytea(sc.DeployerAccountID)
			if err != nil {
				return nil, fmt.Errorf("converting deployer_account_id: %w", err)
			}
			funderBytes, err := pgtypeBytesFromNullAddressBytea(sc.FunderAccountID)
			if err != nil {
				return nil, fmt.Errorf("converting funder_account_id: %w", err)
			}
			destinationBytes, err := pgtypeBytesFromNullAddressBytea(sc.DestinationAccountID)
			if err != nil {
				return nil, fmt.Errorf("converting destination_account_id: %w", err)
			}
			tokenBytes, err := pgtypeBytesFromNullAddressBytea(sc.TokenID)
			if err != nil {
				return nil, fmt.Errorf("converting token_id: %w", err)
			}

			return []any{
				pgtype.Int8{Int64: sc.ToID, Valid: true},
				pgtype.Int8{Int64: sc.StateChangeID, Valid: true},
				pgtype.Text{String: string(sc.StateChangeCategory), Valid: true},
				pgtypeTextFromReason(sc.StateChangeReason),
				pgtype.Timestamptz{Time: sc.LedgerCreatedAt, Valid: true},
				pgtype.Int4{Int32: int32(sc.LedgerNumber), Valid: true},
				accountBytes,
				pgtype.Int8{Int64: sc.OperationID, Valid: true},
				tokenBytes,
				pgtypeTextFromNullString(sc.Amount),
				signerBytes,
				spenderBytes,
				sponsoredBytes,
				sponsorBytes,
				deployerBytes,
				funderBytes,
				destinationBytes,
				pgtypeTextFromNullString(sc.ClaimableBalanceID),
				pgtypeTextFromNullString(sc.LiquidityPoolID),
				pgtypeTextFromNullString(sc.SponsoredData),
				pgtypeInt2FromNullInt16(sc.SignerWeightOld),
				pgtypeInt2FromNullInt16(sc.SignerWeightNew),
				pgtypeInt2FromNullInt16(sc.ThresholdOld),
				pgtypeInt2FromNullInt16(sc.ThresholdNew),
				pgtypeTextFromNullString(sc.TrustlineLimitOld),
				pgtypeTextFromNullString(sc.TrustlineLimitNew),
				pgtypeInt2FromNullInt16(sc.Flags),
				jsonbFromMap(sc.KeyValue),
				pgtypeTextFromNullString(sc.ToMuxedID),
			}, nil
		}),
	)
	if err != nil {
		duration := time.Since(start).Seconds()
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "state_changes").Observe(duration)
		m.Metrics.BatchSize.WithLabelValues("BatchCopy", "state_changes").Observe(float64(len(stateChanges)))
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "state_changes").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "state_changes", utils.GetDBErrorType(err)).Inc()
		return 0, fmt.Errorf("pgx CopyFrom state_changes: %w", err)
	}
	if int(copyCount) != len(stateChanges) {
		duration := time.Since(start).Seconds()
		m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "state_changes").Observe(duration)
		m.Metrics.BatchSize.WithLabelValues("BatchCopy", "state_changes").Observe(float64(len(stateChanges)))
		m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "state_changes").Inc()
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "state_changes", "row_count_mismatch").Inc()
		return 0, fmt.Errorf("expected %d rows copied, got %d", len(stateChanges), copyCount)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "state_changes").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchCopy", "state_changes").Observe(float64(len(stateChanges)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "state_changes").Inc()

	return len(stateChanges), nil
}

// BatchGetByToID gets state changes for a single transaction with pagination support, pinned to
// the parent transaction's ledger_created_at for partition-column chunk exclusion.
func (m *StateChangeModel) BatchGetByToID(ctx context.Context, toID int64, ledgerCreatedAt time.Time, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "operation_id", "state_change_id", "account_id", "ledger_created_at")
	var queryBuilder strings.Builder
	fmt.Fprintf(&queryBuilder, `
		SELECT %s, ledger_created_at as cursor_ledger_created_at, to_id as cursor_to_id, operation_id as cursor_operation_id, state_change_id as cursor_state_change_id
		FROM state_changes
		WHERE to_id = $1 AND ledger_created_at = $2
	`, columns)

	args := []interface{}{toID, ledgerCreatedAt}
	argIndex := 3

	// Decomposed cursor pagination: expands ROW() tuple comparison into OR clauses so
	// TimescaleDB ColumnarScan can push filters into vectorized batch processing.
	if cursor != nil {
		clause, cursorArgs, nextIdx := buildDecomposedCursorCondition([]CursorColumn{
			{Name: "ledger_created_at", Value: cursor.LedgerCreatedAt},
			{Name: "to_id", Value: cursor.ToID},
			{Name: "operation_id", Value: cursor.OperationID},
			{Name: "state_change_id", Value: cursor.StateChangeID},
		}, sortOrder, argIndex)
		queryBuilder.WriteString(" AND " + clause)
		args = append(args, cursorArgs...)
		argIndex = nextIdx
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY ledger_created_at DESC, to_id DESC, operation_id DESC, state_change_id DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY ledger_created_at ASC, to_id ASC, operation_id ASC, state_change_id ASC")
	}

	if limit != nil && *limit > 0 {
		fmt.Fprintf(&queryBuilder, " LIMIT $%d", argIndex)
		args = append(args, *limit)
	}

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order.
	// We use cursor alias columns (e.g., "cursor_ledger_created_at") in ORDER BY to avoid
	// ambiguity since the inner SELECT includes both original columns and cursor aliases.
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY statechanges.cursor_ledger_created_at ASC, statechanges.cursor_to_id ASC, statechanges.cursor_operation_id ASC, statechanges.cursor_state_change_id ASC`, query)
	}

	start := time.Now()
	stateChanges, err := db.QueryManyPtrs[types.StateChangeWithCursor](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByToID", "state_changes").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByToID", "state_changes").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByToID", "state_changes", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting paginated state changes by to_id: %w", err)
	}
	return stateChanges, nil
}

// BatchGetByToIDs gets the state changes that are associated with the given to_ids. Callers
// supply each to_id's parent ledger_created_at so the LATERAL can pin the partition column,
// giving per-key runtime chunk exclusion instead of a hash join across the whole hypertable.
func (m *StateChangeModel) BatchGetByToIDs(ctx context.Context, toIDs []int64, ledgerCreatedAts []time.Time, columns string, limit *int32, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	if len(toIDs) != len(ledgerCreatedAts) {
		return nil, fmt.Errorf("toIDs and ledgerCreatedAts must be parallel arrays of equal length, got %d and %d", len(toIDs), len(ledgerCreatedAts))
	}
	columns = prepareColumnsWithID(columns, types.StateChange{}, "sc", "to_id", "operation_id", "state_change_id", "account_id", "ledger_created_at")

	// The ORDER BY + LIMIT live inside the LATERAL (a per-transaction top-N), replacing the old
	// ROW_NUMBER()-over-everything CTE with an equivalent per-to_id cap; a subquery containing
	// LIMIT cannot be flattened into a join, so the planner must run this as one per-key PK-band
	// probe (see BatchGetByAccountAddress) instead of decompressing the whole hypertable.
	// cursor_ledger_created_at reads k.ledger_created_at (from UNNEST), not sc.ledger_created_at:
	// the LATERAL only projects the caller-requested columns, which may not include
	// ledger_created_at, but k.ledger_created_at is always in scope and identical to
	// sc.ledger_created_at for any row that survives the WHERE match. sc.to_id/operation_id/
	// state_change_id are safe to reference directly since prepareColumnsWithID always forces
	// them into the projection.
	//
	// OFFSET 0 is an optimization fence: it keeps the chunk-selecting scan (which gets runtime
	// chunk exclusion from the ledger_created_at equality) planned separately from the ORDER
	// BY/LIMIT, which would otherwise force a Merge Append probing every chunk's sparse index —
	// O(chunk count) instead of O(1) with retention. After the fence, ORDER BY reads the
	// subquery's bare output column names, not "sc."-qualified ones — there is no "sc" in scope
	// at that level, only the fenced derived table. ledger_created_at and to_id are dropped from
	// the ORDER BY (not just unqualified): both are pinned to a single constant by the WHERE
	// clause within one lateral probe, so they're no-op leading sort keys; operation_id and
	// state_change_id are the only columns that actually vary among a probe's matched rows.
	args := []interface{}{toIDs, ledgerCreatedAts}
	var queryBuilder strings.Builder
	fmt.Fprintf(&queryBuilder, `
		SELECT %s, k.ledger_created_at as cursor_ledger_created_at, sc.to_id as cursor_to_id, sc.operation_id as cursor_operation_id, sc.state_change_id as cursor_state_change_id
		FROM UNNEST($1::bigint[], $2::timestamptz[]) AS k(to_id, ledger_created_at)
		CROSS JOIN LATERAL (
			SELECT * FROM (
				SELECT %s FROM state_changes sc
				WHERE sc.to_id = k.to_id AND sc.ledger_created_at = k.ledger_created_at
				OFFSET 0
			) sub
			ORDER BY operation_id %s, state_change_id %s`, columns, columns, sortOrder, sortOrder)
	if limit != nil {
		queryBuilder.WriteString(" LIMIT $3")
		args = append(args, *limit)
	}
	queryBuilder.WriteString(`
		) sc
	`)

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order.
	// We use cursor alias columns (e.g., "cursor_to_id") in ORDER BY to avoid
	// ambiguity since the inner SELECT includes both original columns and cursor aliases.
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY statechanges.cursor_ledger_created_at ASC, statechanges.cursor_to_id ASC, statechanges.cursor_operation_id ASC, statechanges.cursor_state_change_id ASC`, query)
	}

	start := time.Now()
	stateChanges, err := db.QueryManyPtrs[types.StateChangeWithCursor](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByToIDs", "state_changes").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchGetByToIDs", "state_changes").Observe(float64(len(toIDs)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByToIDs", "state_changes").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByToIDs", "state_changes", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting state changes by to_ids: %w", err)
	}
	return stateChanges, nil
}

// BatchGetByOperationID gets state changes for a single operation with pagination support,
// pinned to the parent operation's ledger_created_at for partition-column chunk exclusion.
func (m *StateChangeModel) BatchGetByOperationID(ctx context.Context, operationID int64, ledgerCreatedAt time.Time, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "operation_id", "state_change_id", "account_id", "ledger_created_at")
	var queryBuilder strings.Builder
	fmt.Fprintf(&queryBuilder, `
		SELECT %s, ledger_created_at as cursor_ledger_created_at, to_id as cursor_to_id, operation_id as cursor_operation_id, state_change_id as cursor_state_change_id
		FROM state_changes
		WHERE operation_id = $1 AND ledger_created_at = $2
	`, columns)

	args := []interface{}{operationID, ledgerCreatedAt}
	argIndex := 3

	// Decomposed cursor pagination: expands ROW() tuple comparison into OR clauses so
	// TimescaleDB ColumnarScan can push filters into vectorized batch processing.
	if cursor != nil {
		clause, cursorArgs, nextIdx := buildDecomposedCursorCondition([]CursorColumn{
			{Name: "ledger_created_at", Value: cursor.LedgerCreatedAt},
			{Name: "to_id", Value: cursor.ToID},
			{Name: "operation_id", Value: cursor.OperationID},
			{Name: "state_change_id", Value: cursor.StateChangeID},
		}, sortOrder, argIndex)
		queryBuilder.WriteString(" AND " + clause)
		args = append(args, cursorArgs...)
		argIndex = nextIdx
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY ledger_created_at DESC, to_id DESC, operation_id DESC, state_change_id DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY ledger_created_at ASC, to_id ASC, operation_id ASC, state_change_id ASC")
	}

	if limit != nil && *limit > 0 {
		fmt.Fprintf(&queryBuilder, " LIMIT $%d", argIndex)
		args = append(args, *limit)
	}

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order.
	// We use cursor alias columns (e.g., "cursor_ledger_created_at") in ORDER BY to avoid
	// ambiguity since the inner SELECT includes both original columns and cursor aliases.
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY statechanges.cursor_ledger_created_at ASC, statechanges.cursor_to_id ASC, statechanges.cursor_operation_id ASC, statechanges.cursor_state_change_id ASC`, query)
	}

	start := time.Now()
	stateChanges, err := db.QueryManyPtrs[types.StateChangeWithCursor](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByOperationID", "state_changes").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByOperationID", "state_changes").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByOperationID", "state_changes", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting paginated state changes by operation ID: %w", err)
	}
	return stateChanges, nil
}

// BatchGetAccountStateChangesByToIDs returns, for each given transaction TOID, all state
// changes in that transaction that belong to accountAddress. toIDs and ledgerCreatedAts are
// parallel arrays; the timestamps are the parent transactions' ledger times (== the state
// changes' ledger times, same ledger).
//
// The query is deliberately a single flat scan, not an UNNEST + LATERAL join: the planner
// may flatten a joinable subquery and then choose a hash join that scans the account's
// entire history instead of probing per transaction. A flat WHERE with to_id = ANY(...)
// plus a ledger_created_at range leaves no join to flatten. to_id alone identifies a
// transaction (it is unique; the pkey includes ledger_created_at only because TimescaleDB
// requires the partition column in unique indexes). The time range exists purely for
// pruning: compressed chunks read only the batches overlapping the page window (segmentby
// account_id, orderby ledger_created_at); uncompressed chunks can use index range scans;
// chunks outside the range are excluded entirely.
func (m *StateChangeModel) BatchGetAccountStateChangesByToIDs(ctx context.Context, accountAddress string, toIDs []int64, ledgerCreatedAts []time.Time, columns string) ([]*types.StateChange, error) {
	if len(toIDs) != len(ledgerCreatedAts) {
		return nil, fmt.Errorf("toIDs and ledgerCreatedAts must be parallel arrays of equal length, got %d and %d", len(toIDs), len(ledgerCreatedAts))
	}
	if len(toIDs) == 0 {
		return []*types.StateChange{}, nil
	}
	lo, hi := ledgerCreatedAts[0], ledgerCreatedAts[0]
	for _, t := range ledgerCreatedAts[1:] {
		if t.Before(lo) {
			lo = t
		}
		if t.After(hi) {
			hi = t
		}
	}
	columns = prepareColumnsWithID(columns, types.StateChange{}, "sc", "to_id", "operation_id", "state_change_id", "account_id", "ledger_created_at")
	query := fmt.Sprintf(`
		SELECT %s
		FROM state_changes sc
		WHERE sc.account_id = $1
		  AND sc.to_id = ANY($2::bigint[])
		  AND sc.ledger_created_at >= $3
		  AND sc.ledger_created_at <= $4
		ORDER BY sc.ledger_created_at DESC, sc.to_id DESC, sc.operation_id DESC, sc.state_change_id DESC
	`, columns)

	start := time.Now()
	stateChanges, err := db.QueryManyPtrs[types.StateChange](ctx, m.DB, query, types.AddressBytea(accountAddress), toIDs, lo, hi)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetAccountStateChangesByToIDs", "state_changes").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchGetAccountStateChangesByToIDs", "state_changes").Observe(float64(len(toIDs)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetAccountStateChangesByToIDs", "state_changes").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetAccountStateChangesByToIDs", "state_changes", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting account state changes by to_ids: %w", err)
	}
	return stateChanges, nil
}

// BatchGetByOperationIDs gets the state changes that are associated with the given operation IDs.
// Callers supply each operation's parent ledger_created_at so the LATERAL can pin the partition
// column, giving per-key runtime chunk exclusion instead of a hash join across the whole
// hypertable.
func (m *StateChangeModel) BatchGetByOperationIDs(ctx context.Context, operationIDs []int64, ledgerCreatedAts []time.Time, columns string, limit *int32, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	if len(operationIDs) != len(ledgerCreatedAts) {
		return nil, fmt.Errorf("operationIDs and ledgerCreatedAts must be parallel arrays of equal length, got %d and %d", len(operationIDs), len(ledgerCreatedAts))
	}
	columns = prepareColumnsWithID(columns, types.StateChange{}, "sc", "to_id", "operation_id", "state_change_id", "account_id", "ledger_created_at")

	// The ORDER BY + LIMIT live inside the LATERAL (a per-operation top-N), replacing the old
	// ROW_NUMBER()-over-everything CTE with an equivalent per-operation cap; a subquery
	// containing LIMIT cannot be flattened into a join, so the planner must run this as one
	// per-key probe (see BatchGetByAccountAddress) instead of decompressing the whole hypertable.
	// cursor_ledger_created_at reads k.ledger_created_at (from UNNEST), not sc.ledger_created_at:
	// the LATERAL only projects the caller-requested columns, which may not include
	// ledger_created_at, but k.ledger_created_at is always in scope and identical to
	// sc.ledger_created_at for any row that survives the WHERE match.
	//
	// OFFSET 0 is an optimization fence: it keeps the chunk-selecting scan (which gets runtime
	// chunk exclusion from the ledger_created_at equality) planned separately from the ORDER
	// BY/LIMIT, which would otherwise force a Merge Append probing every chunk's sparse index —
	// O(chunk count) instead of O(1) with retention. After the fence, ORDER BY reads the
	// subquery's bare output column names, not "sc."-qualified ones — there is no "sc" in scope
	// at that level, only the fenced derived table. ledger_created_at and operation_id are
	// dropped from the ORDER BY (not just unqualified): both are pinned to a single constant by
	// the WHERE clause within one lateral probe, so they're no-op leading sort keys; to_id and
	// state_change_id are the only columns that actually vary among a probe's matched rows.
	args := []interface{}{operationIDs, ledgerCreatedAts}
	var queryBuilder strings.Builder
	fmt.Fprintf(&queryBuilder, `
		SELECT %s, k.ledger_created_at as cursor_ledger_created_at, sc.to_id as cursor_to_id, sc.operation_id as cursor_operation_id, sc.state_change_id as cursor_state_change_id
		FROM UNNEST($1::bigint[], $2::timestamptz[]) AS k(operation_id, ledger_created_at)
		CROSS JOIN LATERAL (
			SELECT * FROM (
				SELECT %s FROM state_changes sc
				WHERE sc.operation_id = k.operation_id AND sc.ledger_created_at = k.ledger_created_at
				OFFSET 0
			) sub
			ORDER BY to_id %s, state_change_id %s`, columns, columns, sortOrder, sortOrder)
	if limit != nil {
		queryBuilder.WriteString(" LIMIT $3")
		args = append(args, *limit)
	}
	queryBuilder.WriteString(`
		) sc
	`)

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order.
	// We use cursor alias columns (e.g., "cursor_to_id") in ORDER BY to avoid
	// ambiguity since the inner SELECT includes both original columns and cursor aliases.
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY statechanges.cursor_ledger_created_at ASC, statechanges.cursor_to_id ASC, statechanges.cursor_operation_id ASC, statechanges.cursor_state_change_id ASC`, query)
	}

	start := time.Now()
	stateChanges, err := db.QueryManyPtrs[types.StateChangeWithCursor](ctx, m.DB, query, args...)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchGetByOperationIDs", "state_changes").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchGetByOperationIDs", "state_changes").Observe(float64(len(operationIDs)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchGetByOperationIDs", "state_changes").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchGetByOperationIDs", "state_changes", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("getting state changes by operation IDs: %w", err)
	}
	return stateChanges, nil
}
