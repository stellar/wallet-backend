package data

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type StateChangeModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

// BatchGetByAccountAddress gets the state changes that are associated with the given account address.
// Optional filters: txHash, operationID, category, and reason can be used to further filter results.
func (m *StateChangeModel) BatchGetByAccountAddress(ctx context.Context, accountAddress string, txHash *string, operationID *int64, category *string, reason *string, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "operation_id", "state_change_order")
	var queryBuilder strings.Builder
	args := []interface{}{types.AddressBytea(accountAddress)}
	argIndex := 2

	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, to_id as "cursor.cursor_to_id", operation_id as "cursor.cursor_operation_id", state_change_order as "cursor.cursor_state_change_order"
		FROM state_changes
		WHERE account_id = $1
	`, columns))

	// Add transaction hash filter if provided (uses subquery to find to_id by hash)
	if txHash != nil {
		queryBuilder.WriteString(fmt.Sprintf(" AND to_id = (SELECT to_id FROM transactions WHERE hash = $%d)", argIndex))
		args = append(args, types.HashBytea(*txHash))
		argIndex++
	}

	// Add operation ID filter if provided
	if operationID != nil {
		queryBuilder.WriteString(fmt.Sprintf(" AND operation_id = $%d", argIndex))
		args = append(args, *operationID)
		argIndex++
	}

	// Add category filter if provided
	if category != nil {
		queryBuilder.WriteString(fmt.Sprintf(" AND state_change_category = $%d", argIndex))
		args = append(args, *category)
		argIndex++
	}

	// Add reason filter if provided
	if reason != nil {
		queryBuilder.WriteString(fmt.Sprintf(" AND state_change_reason = $%d", argIndex))
		args = append(args, *reason)
		argIndex++
	}

	// Add cursor-based pagination using 3-column comparison (to_id, operation_id, state_change_order)
	if cursor != nil {
		if sortOrder == DESC {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (to_id, operation_id, state_change_order) < ($%d, $%d, $%d)
			`, argIndex, argIndex+1, argIndex+2))
			args = append(args, cursor.ToID, cursor.OperationID, cursor.StateChangeOrder)
			argIndex += 3
		} else {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (to_id, operation_id, state_change_order) > ($%d, $%d, $%d)
			`, argIndex, argIndex+1, argIndex+2))
			args = append(args, cursor.ToID, cursor.OperationID, cursor.StateChangeOrder)
			argIndex += 3
		}
	}

	// TODO: Extract the ordering code to separate function in utils and use everywhere
	// Add ordering
	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY to_id DESC, operation_id DESC, state_change_order DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY to_id ASC, operation_id ASC, state_change_order ASC")
	}

	// Add limit using parameterized query
	if limit != nil && *limit > 0 {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT $%d", argIndex))
		args = append(args, *limit)
	}

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order.
	// We use cursor alias columns (e.g., "cursor.cursor_to_id") in ORDER BY to avoid
	// ambiguity since the inner SELECT includes both original columns and cursor aliases.
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY statechanges."cursor.cursor_to_id" ASC, statechanges."cursor.cursor_operation_id" ASC, statechanges."cursor.cursor_state_change_order" ASC`, query)
	}

	var stateChanges []*types.StateChangeWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &stateChanges, query, args...)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByAccountAddress", "state_changes", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByAccountAddress", "state_changes", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting state changes by account address: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByAccountAddress", "state_changes")
	return stateChanges, nil
}

func (m *StateChangeModel) GetAll(ctx context.Context, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "operation_id", "state_change_order")
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, to_id as "cursor.cursor_to_id", operation_id as "cursor.cursor_operation_id", state_change_order as "cursor.cursor_state_change_order"
		FROM state_changes
	`, columns))

	if cursor != nil {
		if sortOrder == DESC {
			queryBuilder.WriteString(fmt.Sprintf(`
				WHERE (to_id, operation_id, state_change_order) < (%d, %d, %d)
			`, cursor.ToID, cursor.OperationID, cursor.StateChangeOrder))
		} else {
			queryBuilder.WriteString(fmt.Sprintf(`
				WHERE (to_id, operation_id, state_change_order) > (%d, %d, %d)
			`, cursor.ToID, cursor.OperationID, cursor.StateChangeOrder))
		}
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY to_id DESC, operation_id DESC, state_change_order DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY to_id ASC, operation_id ASC, state_change_order ASC")
	}

	if limit != nil && *limit > 0 {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT %d", *limit))
	}

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order.
	// We use cursor alias columns (e.g., "cursor.cursor_to_id") in ORDER BY to avoid
	// ambiguity since the inner SELECT includes both original columns and cursor aliases.
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY statechanges."cursor.cursor_to_id" ASC, statechanges."cursor.cursor_operation_id" ASC, statechanges."cursor.cursor_state_change_order" ASC`, query)
	}

	var stateChanges []*types.StateChangeWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &stateChanges, query)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetAll", "state_changes", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetAll", "state_changes", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting all state changes: %w", err)
	}
	m.MetricsService.IncDBQuery("GetAll", "state_changes")
	return stateChanges, nil
}

// BatchCopy inserts state changes using pgx's binary COPY protocol.
// Uses pgx.Tx for binary format which is faster than lib/pq's text format.
// Uses native pgtype types for optimal performance (see https://github.com/jackc/pgx/issues/763).
//
// IMPORTANT: BatchCopy will FAIL if any duplicate records exist. The PostgreSQL COPY
// protocol does not support conflict handling. Callers must ensure no duplicates exist
// before calling this method, or handle the unique constraint violation error appropriately.
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
			"to_id", "state_change_order", "state_change_category", "state_change_reason",
			"ledger_created_at", "ledger_number", "account_id", "operation_id",
			"token_id", "amount", "signer_account_id", "spender_account_id",
			"sponsored_account_id", "sponsor_account_id", "deployer_account_id", "funder_account_id",
			"claimable_balance_id", "liquidity_pool_id", "sponsored_data",
			"signer_weight_old", "signer_weight_new", "threshold_old", "threshold_new",
			"trustline_limit_old", "trustline_limit_new", "flags", "key_value",
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
			tokenBytes, err := pgtypeBytesFromNullAddressBytea(sc.TokenID)
			if err != nil {
				return nil, fmt.Errorf("converting token_id: %w", err)
			}

			return []any{
				pgtype.Int8{Int64: sc.ToID, Valid: true},
				pgtype.Int8{Int64: sc.StateChangeOrder, Valid: true},
				pgtype.Text{String: string(sc.StateChangeCategory), Valid: true},
				pgtypeTextFromReasonPtr(sc.StateChangeReason),
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
			}, nil
		}),
	)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchCopy", "state_changes", utils.GetDBErrorType(err))
		return 0, fmt.Errorf("pgx CopyFrom state_changes: %w", err)
	}
	if int(copyCount) != len(stateChanges) {
		return 0, fmt.Errorf("expected %d rows copied, got %d", len(stateChanges), copyCount)
	}

	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchCopy", "state_changes", duration)
	m.MetricsService.ObserveDBBatchSize("BatchCopy", "state_changes", len(stateChanges))
	m.MetricsService.IncDBQuery("BatchCopy", "state_changes")

	return len(stateChanges), nil
}

// BatchGetByToID gets state changes for a single transaction with pagination support.
func (m *StateChangeModel) BatchGetByToID(ctx context.Context, toID int64, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "operation_id", "state_change_order")
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, to_id as "cursor.cursor_to_id", operation_id as "cursor.cursor_operation_id", state_change_order as "cursor.cursor_state_change_order"
		FROM state_changes
		WHERE to_id = $1
	`, columns))

	args := []interface{}{toID}
	argIndex := 2

	if cursor != nil {
		if sortOrder == DESC {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (to_id, operation_id, state_change_order) < ($%d, $%d, $%d)
			`, argIndex, argIndex+1, argIndex+2))
			args = append(args, cursor.ToID, cursor.OperationID, cursor.StateChangeOrder)
			argIndex += 3
		} else {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (to_id, operation_id, state_change_order) > ($%d, $%d, $%d)
			`, argIndex, argIndex+1, argIndex+2))
			args = append(args, cursor.ToID, cursor.OperationID, cursor.StateChangeOrder)
			argIndex += 3
		}
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY to_id DESC, operation_id DESC, state_change_order DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY to_id ASC, operation_id ASC, state_change_order ASC")
	}

	if limit != nil && *limit > 0 {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT $%d", argIndex))
		args = append(args, *limit)
	}

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order.
	// We use cursor alias columns (e.g., "cursor.cursor_to_id") in ORDER BY to avoid
	// ambiguity since the inner SELECT includes both original columns and cursor aliases.
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY statechanges."cursor.cursor_to_id" ASC, statechanges."cursor.cursor_operation_id" ASC, statechanges."cursor.cursor_state_change_order" ASC`, query)
	}

	var stateChanges []*types.StateChangeWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &stateChanges, query, args...)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByToID", "state_changes", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByToID", "state_changes", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting paginated state changes by to_id: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByToID", "state_changes")
	return stateChanges, nil
}

// BatchGetByToIDs gets the state changes that are associated with the given to_ids.
func (m *StateChangeModel) BatchGetByToIDs(ctx context.Context, toIDs []int64, columns string, limit *int32, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "operation_id", "state_change_order")
	var queryBuilder strings.Builder
	// This CTE query implements per-transaction pagination to ensure balanced results.
	// Instead of applying a global LIMIT that could return all state changes from just a few
	// transactions, we use ROW_NUMBER() with PARTITION BY to_id to limit results per transaction.
	// This guarantees that each transaction gets at most 'limit' state changes, providing
	// more balanced and predictable pagination across multiple transactions.
	queryBuilder.WriteString(fmt.Sprintf(`
		WITH
			inputs (to_id) AS (
				SELECT * FROM UNNEST($1::bigint[])
			),

			ranked_state_changes_per_to_id AS (
				SELECT
					sc.*,
					ROW_NUMBER() OVER (PARTITION BY sc.to_id ORDER BY sc.to_id %s, sc.operation_id %s, sc.state_change_order %s) AS rn
				FROM
					state_changes sc
				JOIN
					inputs i ON sc.to_id = i.to_id
			)
		SELECT %s, to_id as "cursor.cursor_to_id", operation_id as "cursor.cursor_operation_id", state_change_order as "cursor.cursor_state_change_order" FROM ranked_state_changes_per_to_id
	`, sortOrder, sortOrder, sortOrder, columns))
	if limit != nil {
		queryBuilder.WriteString(fmt.Sprintf(" WHERE rn <= %d", *limit))
	}
	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order.
	// We use cursor alias columns (e.g., "cursor.cursor_to_id") in ORDER BY to avoid
	// ambiguity since the inner SELECT includes both original columns and cursor aliases.
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY statechanges."cursor.cursor_to_id" ASC, statechanges."cursor.cursor_operation_id" ASC, statechanges."cursor.cursor_state_change_order" ASC`, query)
	}

	var stateChanges []*types.StateChangeWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &stateChanges, query, pq.Array(toIDs))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByToIDs", "state_changes", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByToIDs", "state_changes", len(toIDs))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByToIDs", "state_changes", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting state changes by to_ids: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByToIDs", "state_changes")
	return stateChanges, nil
}

// BatchGetByOperationID gets state changes for a single operation with pagination support.
func (m *StateChangeModel) BatchGetByOperationID(ctx context.Context, operationID int64, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "operation_id", "state_change_order")
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, to_id as "cursor.cursor_to_id", operation_id as "cursor.cursor_operation_id", state_change_order as "cursor.cursor_state_change_order"
		FROM state_changes
		WHERE operation_id = $1
	`, columns))

	args := []interface{}{operationID}
	argIndex := 2

	if cursor != nil {
		if sortOrder == DESC {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (to_id, operation_id, state_change_order) < ($%d, $%d, $%d)
			`, argIndex, argIndex+1, argIndex+2))
			args = append(args, cursor.ToID, cursor.OperationID, cursor.StateChangeOrder)
			argIndex += 3
		} else {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (to_id, operation_id, state_change_order) > ($%d, $%d, $%d)
			`, argIndex, argIndex+1, argIndex+2))
			args = append(args, cursor.ToID, cursor.OperationID, cursor.StateChangeOrder)
			argIndex += 3
		}
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY to_id DESC, operation_id DESC, state_change_order DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY to_id ASC, operation_id ASC, state_change_order ASC")
	}

	if limit != nil && *limit > 0 {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT $%d", argIndex))
		args = append(args, *limit)
	}

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order.
	// We use cursor alias columns (e.g., "cursor.cursor_to_id") in ORDER BY to avoid
	// ambiguity since the inner SELECT includes both original columns and cursor aliases.
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY statechanges."cursor.cursor_to_id" ASC, statechanges."cursor.cursor_operation_id" ASC, statechanges."cursor.cursor_state_change_order" ASC`, query)
	}

	var stateChanges []*types.StateChangeWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &stateChanges, query, args...)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByOperationID", "state_changes", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByOperationID", "state_changes", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting paginated state changes by operation ID: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByOperationID", "state_changes")
	return stateChanges, nil
}

// BatchGetByOperationIDs gets the state changes that are associated with the given operation IDs.
func (m *StateChangeModel) BatchGetByOperationIDs(ctx context.Context, operationIDs []int64, columns string, limit *int32, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "operation_id", "state_change_order")
	var queryBuilder strings.Builder
	// This CTE query implements per-operation pagination to ensure balanced results.
	// Instead of applying a global LIMIT that could return all state changes from just a few
	// operations, we use ROW_NUMBER() with PARTITION BY operation_id to limit results per operation.
	// This guarantees that each operation gets at most 'limit' state changes, providing
	// more balanced and predictable pagination across multiple operations.
	queryBuilder.WriteString(fmt.Sprintf(`
		WITH
			inputs (operation_id) AS (
				SELECT * FROM UNNEST($1::bigint[])
			),

			ranked_state_changes_per_operation_id AS (
				SELECT
					sc.*,
					ROW_NUMBER() OVER (PARTITION BY sc.operation_id ORDER BY sc.to_id %s, sc.operation_id %s, sc.state_change_order %s) AS rn
				FROM
					state_changes sc
				JOIN
					inputs i ON sc.operation_id = i.operation_id
			)
		SELECT %s, to_id as "cursor.cursor_to_id", operation_id as "cursor.cursor_operation_id", state_change_order as "cursor.cursor_state_change_order" FROM ranked_state_changes_per_operation_id
	`, sortOrder, sortOrder, sortOrder, columns))
	if limit != nil {
		queryBuilder.WriteString(fmt.Sprintf(" WHERE rn <= %d", *limit))
	}
	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order.
	// We use cursor alias columns (e.g., "cursor.cursor_to_id") in ORDER BY to avoid
	// ambiguity since the inner SELECT includes both original columns and cursor aliases.
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY statechanges."cursor.cursor_to_id" ASC, statechanges."cursor.cursor_operation_id" ASC, statechanges."cursor.cursor_state_change_order" ASC`, query)
	}

	var stateChanges []*types.StateChangeWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &stateChanges, query, pq.Array(operationIDs))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByOperationIDs", "state_changes", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByOperationIDs", "state_changes", len(operationIDs))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByOperationIDs", "state_changes", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting state changes by operation IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByOperationIDs", "state_changes")
	return stateChanges, nil
}
