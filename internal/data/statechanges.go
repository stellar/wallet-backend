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
	args := []interface{}{accountAddress}
	argIndex := 2

	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, to_id as "cursor.cursor_to_id", operation_id as "cursor.cursor_operation_id", state_change_order as "cursor.cursor_state_change_order"
		FROM state_changes
		WHERE account_id = $1
	`, columns))

	// Add transaction hash filter if provided (uses subquery to find to_id by hash)
	if txHash != nil {
		queryBuilder.WriteString(fmt.Sprintf(" AND to_id = (SELECT to_id FROM transactions WHERE hash = $%d)", argIndex))
		args = append(args, *txHash)
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

func (m *StateChangeModel) BatchInsert(
	ctx context.Context,
	sqlExecuter db.SQLExecuter,
	stateChanges []types.StateChange,
) ([]string, error) {
	if sqlExecuter == nil {
		sqlExecuter = m.DB
	}

	// Flatten the state changes into parallel slices
	stateChangeOrders := make([]int64, len(stateChanges))
	toIDs := make([]int64, len(stateChanges))
	categories := make([]string, len(stateChanges))
	reasons := make([]*string, len(stateChanges))
	ledgerCreatedAts := make([]time.Time, len(stateChanges))
	ledgerNumbers := make([]int, len(stateChanges))
	accountIDBytes := make([][]byte, len(stateChanges))
	operationIDs := make([]int64, len(stateChanges))
	tokenIDs := make([]*string, len(stateChanges))
	amounts := make([]*string, len(stateChanges))
	signerAccountIDBytes := make([][]byte, len(stateChanges))
	spenderAccountIDBytes := make([][]byte, len(stateChanges))
	sponsoredAccountIDBytes := make([][]byte, len(stateChanges))
	sponsorAccountIDBytes := make([][]byte, len(stateChanges))
	deployerAccountIDBytes := make([][]byte, len(stateChanges))
	funderAccountIDBytes := make([][]byte, len(stateChanges))
	claimableBalanceIDs := make([]*string, len(stateChanges))
	liquidityPoolIDs := make([]*string, len(stateChanges))
	sponsoredDataValues := make([]*string, len(stateChanges))
	signerWeightOlds := make([]*int16, len(stateChanges))
	signerWeightNews := make([]*int16, len(stateChanges))
	thresholdOlds := make([]*int16, len(stateChanges))
	thresholdNews := make([]*int16, len(stateChanges))
	trustlineLimitOlds := make([]*string, len(stateChanges))
	trustlineLimitNews := make([]*string, len(stateChanges))
	flags := make([]*int16, len(stateChanges))
	keyValues := make([]*types.NullableJSONB, len(stateChanges))

	for i, sc := range stateChanges {
		stateChangeOrders[i] = sc.StateChangeOrder
		toIDs[i] = sc.ToID
		categories[i] = string(sc.StateChangeCategory)
		ledgerCreatedAts[i] = sc.LedgerCreatedAt
		ledgerNumbers[i] = int(sc.LedgerNumber)
		operationIDs[i] = sc.OperationID

		// Convert account_id to BYTEA (required field)
		addrBytes, err := sc.AccountID.Value()
		if err != nil {
			return nil, fmt.Errorf("converting account_id: %w", err)
		}
		accountIDBytes[i] = addrBytes.([]byte)

		// Nullable fields
		if sc.StateChangeReason != nil {
			reason := string(*sc.StateChangeReason)
			reasons[i] = &reason
		}
		if sc.TokenID.Valid {
			tokenIDs[i] = &sc.TokenID.String
		}
		if sc.Amount.Valid {
			amounts[i] = &sc.Amount.String
		}

		// Convert nullable account_id fields to BYTEA
		signerAccountIDBytes[i], err = pgtypeBytesFromNullStringAddress(sc.SignerAccountID)
		if err != nil {
			return nil, fmt.Errorf("converting signer_account_id: %w", err)
		}
		spenderAccountIDBytes[i], err = pgtypeBytesFromNullStringAddress(sc.SpenderAccountID)
		if err != nil {
			return nil, fmt.Errorf("converting spender_account_id: %w", err)
		}
		sponsoredAccountIDBytes[i], err = pgtypeBytesFromNullStringAddress(sc.SponsoredAccountID)
		if err != nil {
			return nil, fmt.Errorf("converting sponsored_account_id: %w", err)
		}
		sponsorAccountIDBytes[i], err = pgtypeBytesFromNullStringAddress(sc.SponsorAccountID)
		if err != nil {
			return nil, fmt.Errorf("converting sponsor_account_id: %w", err)
		}
		deployerAccountIDBytes[i], err = pgtypeBytesFromNullStringAddress(sc.DeployerAccountID)
		if err != nil {
			return nil, fmt.Errorf("converting deployer_account_id: %w", err)
		}
		funderAccountIDBytes[i], err = pgtypeBytesFromNullStringAddress(sc.FunderAccountID)
		if err != nil {
			return nil, fmt.Errorf("converting funder_account_id: %w", err)
		}
		if sc.ClaimableBalanceID.Valid {
			claimableBalanceIDs[i] = &sc.ClaimableBalanceID.String
		}
		if sc.LiquidityPoolID.Valid {
			liquidityPoolIDs[i] = &sc.LiquidityPoolID.String
		}
		if sc.SponsoredData.Valid {
			sponsoredDataValues[i] = &sc.SponsoredData.String
		}
		if sc.SignerWeightOld.Valid {
			signerWeightOlds[i] = &sc.SignerWeightOld.Int16
		}
		if sc.SignerWeightNew.Valid {
			signerWeightNews[i] = &sc.SignerWeightNew.Int16
		}
		if sc.ThresholdOld.Valid {
			thresholdOlds[i] = &sc.ThresholdOld.Int16
		}
		if sc.ThresholdNew.Valid {
			thresholdNews[i] = &sc.ThresholdNew.Int16
		}
		if sc.TrustlineLimitOld.Valid {
			trustlineLimitOlds[i] = &sc.TrustlineLimitOld.String
		}
		if sc.TrustlineLimitNew.Valid {
			trustlineLimitNews[i] = &sc.TrustlineLimitNew.String
		}
		if sc.Flags.Valid {
			flags[i] = &sc.Flags.Int16
		}
		if sc.KeyValue != nil {
			keyValues[i] = &sc.KeyValue
		}
	}

	const insertQuery = `
		-- Insert state changes
		WITH input_data AS (
			SELECT
				UNNEST($1::bigint[]) AS state_change_order,
				UNNEST($2::bigint[]) AS to_id,
				UNNEST($3::text[]) AS state_change_category,
				UNNEST($4::text[]) AS state_change_reason,
				UNNEST($5::timestamptz[]) AS ledger_created_at,
				UNNEST($6::integer[]) AS ledger_number,
				UNNEST($7::bytea[]) AS account_id,
				UNNEST($8::bigint[]) AS operation_id,
				UNNEST($9::text[]) AS token_id,
				UNNEST($10::text[]) AS amount,
				UNNEST($11::bytea[]) AS signer_account_id,
				UNNEST($12::bytea[]) AS spender_account_id,
				UNNEST($13::bytea[]) AS sponsored_account_id,
				UNNEST($14::bytea[]) AS sponsor_account_id,
				UNNEST($15::bytea[]) AS deployer_account_id,
				UNNEST($16::bytea[]) AS funder_account_id,
				UNNEST($17::text[]) AS claimable_balance_id,
				UNNEST($18::text[]) AS liquidity_pool_id,
				UNNEST($19::text[]) AS sponsored_data,
				UNNEST($20::smallint[]) AS signer_weight_old,
				UNNEST($21::smallint[]) AS signer_weight_new,
				UNNEST($22::smallint[]) AS threshold_old,
				UNNEST($23::smallint[]) AS threshold_new,
				UNNEST($24::text[]) AS trustline_limit_old,
				UNNEST($25::text[]) AS trustline_limit_new,
				UNNEST($26::smallint[]) AS flags,
				UNNEST($27::jsonb[]) AS key_value
		),
		inserted_state_changes AS (
			INSERT INTO state_changes
				(state_change_order, to_id, state_change_category, state_change_reason, ledger_created_at,
				ledger_number, account_id, operation_id, token_id, amount,
				signer_account_id, spender_account_id, sponsored_account_id, sponsor_account_id,
				deployer_account_id, funder_account_id, claimable_balance_id, liquidity_pool_id, sponsored_data,
				signer_weight_old, signer_weight_new, threshold_old, threshold_new,
				trustline_limit_old, trustline_limit_new, flags, key_value)
			SELECT
				sc.state_change_order, sc.to_id, sc.state_change_category, sc.state_change_reason, sc.ledger_created_at,
				sc.ledger_number, sc.account_id, sc.operation_id, sc.token_id, sc.amount,
				sc.signer_account_id, sc.spender_account_id, sc.sponsored_account_id, sc.sponsor_account_id,
				sc.deployer_account_id, sc.funder_account_id, sc.claimable_balance_id, sc.liquidity_pool_id, sc.sponsored_data,
				sc.signer_weight_old, sc.signer_weight_new, sc.threshold_old, sc.threshold_new,
				sc.trustline_limit_old, sc.trustline_limit_new, sc.flags, sc.key_value
			FROM input_data sc
			ON CONFLICT (to_id, operation_id, state_change_order) DO NOTHING
			RETURNING to_id, operation_id, state_change_order
		)
		SELECT CONCAT(to_id, '-', operation_id, '-', state_change_order) FROM inserted_state_changes;
	`

	start := time.Now()
	var insertedIDs []string
	err := sqlExecuter.SelectContext(ctx, &insertedIDs, insertQuery,
		pq.Array(stateChangeOrders),
		pq.Array(toIDs),
		pq.Array(categories),
		pq.Array(reasons),
		pq.Array(ledgerCreatedAts),
		pq.Array(ledgerNumbers),
		pq.Array(accountIDBytes),
		pq.Array(operationIDs),
		pq.Array(tokenIDs),
		pq.Array(amounts),
		pq.Array(signerAccountIDBytes),
		pq.Array(spenderAccountIDBytes),
		pq.Array(sponsoredAccountIDBytes),
		pq.Array(sponsorAccountIDBytes),
		pq.Array(deployerAccountIDBytes),
		pq.Array(funderAccountIDBytes),
		pq.Array(claimableBalanceIDs),
		pq.Array(liquidityPoolIDs),
		pq.Array(sponsoredDataValues),
		pq.Array(signerWeightOlds),
		pq.Array(signerWeightNews),
		pq.Array(thresholdOlds),
		pq.Array(thresholdNews),
		pq.Array(trustlineLimitOlds),
		pq.Array(trustlineLimitNews),
		pq.Array(flags),
		pq.Array(keyValues),
	)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchInsert", "state_changes", duration)
	m.MetricsService.ObserveDBBatchSize("BatchInsert", "state_changes", len(stateChanges))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchInsert", "state_changes", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("batch inserting state changes: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchInsert", "state_changes")

	return insertedIDs, nil
}

// BatchCopy inserts state changes using pgx's binary COPY protocol.
// Uses pgx.Tx for binary format which is faster than lib/pq's text format.
// Uses native pgtype types for optimal performance (see https://github.com/jackc/pgx/issues/763).
//
// IMPORTANT: Unlike BatchInsert which uses ON CONFLICT DO NOTHING, BatchCopy will FAIL
// if any duplicate records exist. The PostgreSQL COPY protocol does not support conflict
// handling. Callers must ensure no duplicates exist before calling this method, or handle
// the unique constraint violation error appropriately.
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
			return []any{
				pgtype.Int8{Int64: sc.ToID, Valid: true},
				pgtype.Int8{Int64: sc.StateChangeOrder, Valid: true},
				pgtype.Text{String: string(sc.StateChangeCategory), Valid: true},
				pgtypeTextFromReasonPtr(sc.StateChangeReason),
				pgtype.Timestamptz{Time: sc.LedgerCreatedAt, Valid: true},
				pgtype.Int4{Int32: int32(sc.LedgerNumber), Valid: true},
				pgtype.Text{String: sc.AccountID, Valid: true},
				pgtype.Int8{Int64: sc.OperationID, Valid: true},
				pgtypeTextFromNullString(sc.TokenID),
				pgtypeTextFromNullString(sc.Amount),
				pgtypeTextFromNullString(sc.SignerAccountID),
				pgtypeTextFromNullString(sc.SpenderAccountID),
				pgtypeTextFromNullString(sc.SponsoredAccountID),
				pgtypeTextFromNullString(sc.SponsorAccountID),
				pgtypeTextFromNullString(sc.DeployerAccountID),
				pgtypeTextFromNullString(sc.FunderAccountID),
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
				AND (to_id, operation_id, state_change_order) < (%d, %d, %d)
			`, cursor.ToID, cursor.OperationID, cursor.StateChangeOrder))
		} else {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (to_id, operation_id, state_change_order) > (%d, %d, %d)
			`, cursor.ToID, cursor.OperationID, cursor.StateChangeOrder))
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
				AND (to_id, operation_id, state_change_order) < (%d, %d, %d)
			`, cursor.ToID, cursor.OperationID, cursor.StateChangeOrder))
		} else {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (to_id, operation_id, state_change_order) > (%d, %d, %d)
			`, cursor.ToID, cursor.OperationID, cursor.StateChangeOrder))
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
