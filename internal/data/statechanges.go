package data

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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
// txHash filter uses JOIN with transactions table since tx_hash column was removed from state_changes.
// operationID filter uses to_id directly since for operation-related state changes, to_id = operation_id.
func (m *StateChangeModel) BatchGetByAccountAddress(ctx context.Context, accountAddress string, txHash *string, operationID *int64, category *string, reason *string, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "sc", "to_id", "state_change_order")
	var queryBuilder strings.Builder
	args := []interface{}{types.StellarAddress(accountAddress)}
	argIndex := 2

	// Use JOIN with transactions table when txHash filter is provided
	if txHash != nil {
		queryBuilder.WriteString(fmt.Sprintf(`
			SELECT %s, sc.to_id as "cursor.cursor_to_id", sc.state_change_order as "cursor.cursor_state_change_order",
				t.hash as tx_hash, sc.to_id as operation_id
			FROM state_changes sc
			JOIN transactions t ON (sc.to_id & ~4095) = t.to_id
			WHERE sc.account_id = $1 AND t.hash = $%d
		`, columns, argIndex))
		args = append(args, *txHash)
		argIndex++
	} else {
		queryBuilder.WriteString(fmt.Sprintf(`
			SELECT %s, sc.to_id as "cursor.cursor_to_id", sc.state_change_order as "cursor.cursor_state_change_order",
				'' as tx_hash, sc.to_id as operation_id
			FROM state_changes sc
			WHERE sc.account_id = $1
		`, columns))
	}

	// Add operation ID filter if provided (to_id = operation_id for operation-related state changes)
	if operationID != nil {
		queryBuilder.WriteString(fmt.Sprintf(" AND sc.to_id = $%d", argIndex))
		args = append(args, *operationID)
		argIndex++
	}

	// Add category filter if provided (convert string to SMALLINT for query)
	if category != nil {
		categoryInt := types.StateChangeCategory(*category).ToInt16()
		queryBuilder.WriteString(fmt.Sprintf(" AND sc.state_change_category = $%d", argIndex))
		args = append(args, categoryInt)
		argIndex++
	}

	// Add reason filter if provided (convert string to SMALLINT for query)
	if reason != nil {
		reasonInt := types.StateChangeReason(*reason).ToInt16()
		queryBuilder.WriteString(fmt.Sprintf(" AND sc.state_change_reason = $%d", argIndex))
		args = append(args, reasonInt)
		argIndex++
	}

	// Add cursor-based pagination using parameterized queries
	if cursor != nil {
		if sortOrder == DESC {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (sc.to_id < $%d OR (sc.to_id = $%d AND sc.state_change_order < $%d))
			`, argIndex, argIndex, argIndex+1))
			args = append(args, cursor.ToID, cursor.StateChangeOrder)
			argIndex += 2
		} else {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (sc.to_id > $%d OR (sc.to_id = $%d AND sc.state_change_order > $%d))
			`, argIndex, argIndex, argIndex+1))
			args = append(args, cursor.ToID, cursor.StateChangeOrder)
			argIndex += 2
		}
	}

	// Add ordering
	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY sc.to_id DESC, sc.state_change_order DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY sc.to_id ASC, sc.state_change_order ASC")
	}

	// Add limit using parameterized query
	if limit != nil && *limit > 0 {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT $%d", argIndex))
		args = append(args, *limit)
	}

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY to_id ASC, state_change_order ASC`, query)
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
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "state_change_order")
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, to_id as "cursor.cursor_to_id", state_change_order as "cursor.cursor_state_change_order"
		FROM state_changes
	`, columns))

	if cursor != nil {
		if sortOrder == DESC {
			queryBuilder.WriteString(fmt.Sprintf(`
				WHERE (to_id < %d OR (to_id = %d AND state_change_order < %d))
			`, cursor.ToID, cursor.ToID, cursor.StateChangeOrder))
		} else {
			queryBuilder.WriteString(fmt.Sprintf(`
				WHERE (to_id > %d OR (to_id = %d AND state_change_order > %d))
			`, cursor.ToID, cursor.ToID, cursor.StateChangeOrder))
		}
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY to_id DESC, state_change_order DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY to_id ASC, state_change_order ASC")
	}

	if limit != nil && *limit > 0 {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT %d", *limit))
	}

	query := queryBuilder.String()

	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY to_id ASC, state_change_order ASC`, query)
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
	categories := make([]int16, len(stateChanges))
	reasons := make([]*int16, len(stateChanges))
	ledgerCreatedAts := make([]time.Time, len(stateChanges))
	accountIDs := make([][]byte, len(stateChanges))
	tokenIDs := make([][]byte, len(stateChanges))
	amounts := make([]*string, len(stateChanges))
	offerIDs := make([]*string, len(stateChanges))
	signerAccountIDs := make([][]byte, len(stateChanges))
	spenderAccountIDs := make([][]byte, len(stateChanges))
	sponsoredAccountIDs := make([][]byte, len(stateChanges))
	sponsorAccountIDs := make([][]byte, len(stateChanges))
	deployerAccountIDs := make([][]byte, len(stateChanges))
	funderAccountIDs := make([][]byte, len(stateChanges))
	signerWeights := make([]*types.NullableJSONB, len(stateChanges))
	thresholds := make([]*types.NullableJSONB, len(stateChanges))
	trustlineLimits := make([]*types.NullableJSONB, len(stateChanges))
	flags := make([]*types.NullableJSON, len(stateChanges))
	keyValues := make([]*types.NullableJSONB, len(stateChanges))

	for i, sc := range stateChanges {
		stateChangeOrders[i] = sc.StateChangeOrder
		toIDs[i] = sc.ToID
		categories[i] = sc.StateChangeCategory.ToInt16()
		ledgerCreatedAts[i] = sc.LedgerCreatedAt
		accountIDs[i] = bytesFromStellarAddress(sc.AccountID)

		// Nullable fields
		if sc.StateChangeReason != nil {
			reason := sc.StateChangeReason.ToInt16()
			reasons[i] = &reason
		}
		tokenIDs[i] = bytesFromNullableStellarAddress(sc.TokenID)
		if sc.Amount.Valid {
			amounts[i] = &sc.Amount.String
		}
		if sc.OfferID.Valid {
			offerIDs[i] = &sc.OfferID.String
		}
		signerAccountIDs[i] = bytesFromNullableStellarAddress(sc.SignerAccountID)
		spenderAccountIDs[i] = bytesFromNullableStellarAddress(sc.SpenderAccountID)
		sponsoredAccountIDs[i] = bytesFromNullableStellarAddress(sc.SponsoredAccountID)
		sponsorAccountIDs[i] = bytesFromNullableStellarAddress(sc.SponsorAccountID)
		deployerAccountIDs[i] = bytesFromNullableStellarAddress(sc.DeployerAccountID)
		funderAccountIDs[i] = bytesFromNullableStellarAddress(sc.FunderAccountID)
		if sc.SignerWeights != nil {
			signerWeights[i] = &sc.SignerWeights
		}
		if sc.Thresholds != nil {
			thresholds[i] = &sc.Thresholds
		}
		if sc.TrustlineLimit != nil {
			trustlineLimits[i] = &sc.TrustlineLimit
		}
		if sc.Flags != nil {
			flags[i] = &sc.Flags
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
				UNNEST($3::smallint[]) AS state_change_category,
				UNNEST($4::smallint[]) AS state_change_reason,
				UNNEST($5::timestamptz[]) AS ledger_created_at,
				UNNEST($6::bytea[]) AS account_id,
				UNNEST($7::bytea[]) AS token_id,
				UNNEST($8::text[]) AS amount,
				UNNEST($9::text[]) AS offer_id,
				UNNEST($10::bytea[]) AS signer_account_id,
				UNNEST($11::bytea[]) AS spender_account_id,
				UNNEST($12::bytea[]) AS sponsored_account_id,
				UNNEST($13::bytea[]) AS sponsor_account_id,
				UNNEST($14::bytea[]) AS deployer_account_id,
				UNNEST($15::bytea[]) AS funder_account_id,
				UNNEST($16::jsonb[]) AS signer_weights,
				UNNEST($17::jsonb[]) AS thresholds,
				UNNEST($18::jsonb[]) AS trustline_limit,
				UNNEST($19::jsonb[]) AS flags,
				UNNEST($20::jsonb[]) AS key_value
		),
		inserted_state_changes AS (
			INSERT INTO state_changes
				(state_change_order, to_id, state_change_category, state_change_reason, ledger_created_at,
				account_id, token_id, amount, offer_id, signer_account_id,
				spender_account_id, sponsored_account_id, sponsor_account_id,
				deployer_account_id, funder_account_id, signer_weights, thresholds, trustline_limit, flags, key_value)
			SELECT
				sc.state_change_order, sc.to_id, sc.state_change_category, sc.state_change_reason, sc.ledger_created_at,
				sc.account_id, sc.token_id, sc.amount, sc.offer_id, sc.signer_account_id,
				sc.spender_account_id, sc.sponsored_account_id, sc.sponsor_account_id,
				sc.deployer_account_id, sc.funder_account_id, sc.signer_weights, sc.thresholds, sc.trustline_limit, sc.flags, sc.key_value
			FROM input_data sc
			ON CONFLICT (to_id, state_change_order) DO NOTHING
			RETURNING to_id, state_change_order
		)
		SELECT CONCAT(to_id, '-', state_change_order) FROM inserted_state_changes;
	`

	start := time.Now()
	var insertedIDs []string
	err := sqlExecuter.SelectContext(ctx, &insertedIDs, insertQuery,
		pq.Array(stateChangeOrders),
		pq.Array(toIDs),
		pq.Array(categories),
		pq.Array(reasons),
		pq.Array(ledgerCreatedAts),
		pq.Array(accountIDs),
		pq.Array(tokenIDs),
		pq.Array(amounts),
		pq.Array(offerIDs),
		pq.Array(signerAccountIDs),
		pq.Array(spenderAccountIDs),
		pq.Array(sponsoredAccountIDs),
		pq.Array(sponsorAccountIDs),
		pq.Array(deployerAccountIDs),
		pq.Array(funderAccountIDs),
		pq.Array(signerWeights),
		pq.Array(thresholds),
		pq.Array(trustlineLimits),
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

// pgtypeTextFromNullString converts sql.NullString to pgtype.Text for efficient binary COPY.
func pgtypeTextFromNullString(ns sql.NullString) pgtype.Text {
	return pgtype.Text{String: ns.String, Valid: ns.Valid}
}

// pgtypeInt2FromReasonPtr converts *types.StateChangeReason to pgtype.Int2 for efficient binary COPY.
func pgtypeInt2FromReasonPtr(r *types.StateChangeReason) pgtype.Int2 {
	if r == nil {
		return pgtype.Int2{Valid: false}
	}
	return pgtype.Int2{Int16: r.ToInt16(), Valid: true}
}

// jsonbFromMap converts types.NullableJSONB to any for pgx CopyFrom.
// pgx automatically handles map[string]any → JSONB conversion.
func jsonbFromMap(m types.NullableJSONB) any {
	if m == nil {
		return nil
	}
	// Return the map directly; pgx handles JSON marshaling automatically
	return map[string]any(m)
}

// jsonbFromSlice converts types.NullableJSON to any for pgx CopyFrom.
// pgx automatically handles []string → JSONB conversion.
func jsonbFromSlice(s types.NullableJSON) any {
	if s == nil {
		return nil
	}
	// Return the slice directly; pgx handles JSON marshaling automatically
	return []string(s)
}

// BatchCopy inserts state changes using pgx's binary COPY protocol.
// Uses pgx.Tx for binary format which is faster than lib/pq's text format.
// Uses native pgtype types for optimal performance (see https://github.com/jackc/pgx/issues/763).
func (m *StateChangeModel) BatchCopy(
	ctx context.Context,
	pgxTx pgx.Tx,
	stateChanges []types.StateChange,
) (int, error) {
	if len(stateChanges) == 0 {
		return 0, nil
	}

	start := time.Now()

	var validStateChanges []types.StateChange
	for _, sc := range stateChanges {
		accountID := bytesFromStellarAddress(sc.AccountID)
		if accountID != nil {
			validStateChanges = append(validStateChanges, sc)
		} else {
			log.Printf("Skipping state change with invalid account ID: %s (to_id=%d, order=%d)",
				sc.AccountID, sc.ToID, sc.StateChangeOrder)
		}
	}

	if len(validStateChanges) == 0 {
		log.Printf("All %d state changes had invalid account IDs, skipping batch", len(stateChanges))
		return 0, nil
	}

	if len(validStateChanges) < len(stateChanges) {
		log.Printf("Filtered out %d invalid state changes, proceeding with %d valid ones",
			len(stateChanges)-len(validStateChanges), len(validStateChanges))
	}

	// COPY state_changes using pgx binary format with native pgtype types
	copyCount, err := pgxTx.CopyFrom(
		ctx,
		pgx.Identifier{"state_changes"},
		[]string{
			"to_id", "state_change_order", "state_change_category", "state_change_reason",
			"ledger_created_at", "account_id", "token_id", "amount", "offer_id",
			"signer_account_id", "spender_account_id", "sponsored_account_id", "sponsor_account_id",
			"deployer_account_id", "funder_account_id", "signer_weights", "thresholds",
			"trustline_limit", "flags", "key_value",
		},
		pgx.CopyFromSlice(len(validStateChanges), func(i int) ([]any, error) {
			sc := validStateChanges[i]
			return []any{
				pgtype.Int8{Int64: sc.ToID, Valid: true},
				pgtype.Int8{Int64: sc.StateChangeOrder, Valid: true},
				pgtype.Int2{Int16: sc.StateChangeCategory.ToInt16(), Valid: true},
				pgtypeInt2FromReasonPtr(sc.StateChangeReason),
				pgtype.Timestamptz{Time: sc.LedgerCreatedAt, Valid: true},
				bytesFromStellarAddress(sc.AccountID),
				bytesFromNullableStellarAddress(sc.TokenID),
				pgtypeTextFromNullString(sc.Amount),
				pgtypeTextFromNullString(sc.OfferID),
				bytesFromNullableStellarAddress(sc.SignerAccountID),
				bytesFromNullableStellarAddress(sc.SpenderAccountID),
				bytesFromNullableStellarAddress(sc.SponsoredAccountID),
				bytesFromNullableStellarAddress(sc.SponsorAccountID),
				bytesFromNullableStellarAddress(sc.DeployerAccountID),
				bytesFromNullableStellarAddress(sc.FunderAccountID),
				jsonbFromMap(sc.SignerWeights),
				jsonbFromMap(sc.Thresholds),
				jsonbFromMap(sc.TrustlineLimit),
				jsonbFromSlice(sc.Flags),
				jsonbFromMap(sc.KeyValue),
			}, nil
		}),
	)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchCopy", "state_changes", utils.GetDBErrorType(err))
		return 0, fmt.Errorf("pgx CopyFrom state_changes: %w", err)
	}
	if int(copyCount) != len(validStateChanges) {
		return 0, fmt.Errorf("expected %d rows copied, got %d", len(validStateChanges), copyCount)
	}

	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchCopy", "state_changes", duration)
	m.MetricsService.ObserveDBBatchSize("BatchCopy", "state_changes", len(validStateChanges))
	m.MetricsService.IncDBQuery("BatchCopy", "state_changes")

	return len(validStateChanges), nil
}

// BatchGetByTxHash gets state changes for a single transaction with pagination support.
// Uses JOIN with transactions table since tx_hash column was removed from state_changes.
func (m *StateChangeModel) BatchGetByTxHash(ctx context.Context, txHash string, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "sc", "to_id", "state_change_order")
	var queryBuilder strings.Builder
	// JOIN with transactions table via TOID derivation: (sc.to_id & ~4095) = t.to_id
	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, sc.to_id as "cursor.cursor_to_id", sc.state_change_order as "cursor.cursor_state_change_order", t.hash as tx_hash
		FROM state_changes sc
		JOIN transactions t ON (sc.to_id & ~4095) = t.to_id
		WHERE t.hash = $1
	`, columns))

	args := []interface{}{txHash}
	argIndex := 2

	if cursor != nil {
		if sortOrder == DESC {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (sc.to_id < %d OR (sc.to_id = %d AND sc.state_change_order < %d))
			`, cursor.ToID, cursor.ToID, cursor.StateChangeOrder))
		} else {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (sc.to_id > %d OR (sc.to_id = %d AND sc.state_change_order > %d))
			`, cursor.ToID, cursor.ToID, cursor.StateChangeOrder))
		}
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY sc.to_id DESC, sc.state_change_order DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY sc.to_id ASC, sc.state_change_order ASC")
	}

	if limit != nil && *limit > 0 {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT $%d", argIndex))
		args = append(args, *limit)
	}

	query := queryBuilder.String()

	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY to_id ASC, state_change_order ASC`, query)
	}

	var stateChanges []*types.StateChangeWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &stateChanges, query, args...)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByTxHash", "state_changes", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByTxHash", "state_changes", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting paginated state changes by tx hash: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByTxHash", "state_changes")
	return stateChanges, nil
}

// BatchGetByTxHashes gets the state changes that are associated with the given transaction hashes.
// Uses JOIN with transactions table since tx_hash column was removed from state_changes.
func (m *StateChangeModel) BatchGetByTxHashes(ctx context.Context, txHashes []string, columns string, limit *int32, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "state_change_order")
	var queryBuilder strings.Builder
	// This CTE query implements per-transaction pagination to ensure balanced results.
	// Instead of applying a global LIMIT that could return all state changes from just a few
	// transactions, we use ROW_NUMBER() with PARTITION BY t.hash to limit results per transaction.
	// Uses JOIN with transactions table via TOID derivation: (sc.to_id & ~4095) = t.to_id
	queryBuilder.WriteString(fmt.Sprintf(`
		WITH
			inputs (tx_hash) AS (
				SELECT * FROM UNNEST($1::text[])
			),

			ranked_state_changes_per_tx_hash AS (
				SELECT
					sc.*,
					t.hash as tx_hash,
					sc.to_id as operation_id,
					ROW_NUMBER() OVER (PARTITION BY t.hash ORDER BY sc.to_id %s, sc.state_change_order %s) AS rn
				FROM
					state_changes sc
				JOIN
					transactions t ON (sc.to_id & ~4095) = t.to_id
				JOIN
					inputs i ON t.hash = i.tx_hash
			)
		SELECT %s, to_id as "cursor.cursor_to_id", state_change_order as "cursor.cursor_state_change_order", tx_hash, operation_id FROM ranked_state_changes_per_tx_hash
	`, sortOrder, sortOrder, columns))
	if limit != nil {
		queryBuilder.WriteString(fmt.Sprintf(" WHERE rn <= %d", *limit))
	}
	query := queryBuilder.String()

	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY to_id ASC, state_change_order ASC`, query)
	}

	var stateChanges []*types.StateChangeWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &stateChanges, query, pq.Array(txHashes))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByTxHashes", "state_changes", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByTxHashes", "state_changes", len(txHashes))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByTxHashes", "state_changes", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting state changes by transaction hashes: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByTxHashes", "state_changes")
	return stateChanges, nil
}

// BatchGetByOperationID gets state changes for a single operation with pagination support.
// For operation-related state changes, to_id equals the operation ID (when to_id & 4095 != 0).
func (m *StateChangeModel) BatchGetByOperationID(ctx context.Context, operationID int64, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "state_change_order")
	var queryBuilder strings.Builder
	// For operation-related state changes, to_id IS the operation_id
	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, to_id as "cursor.cursor_to_id", state_change_order as "cursor.cursor_state_change_order", to_id as operation_id
		FROM state_changes
		WHERE to_id = $1
	`, columns))

	args := []interface{}{operationID}
	argIndex := 2

	if cursor != nil {
		if sortOrder == DESC {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (to_id < %d OR (to_id = %d AND state_change_order < %d))
			`, cursor.ToID, cursor.ToID, cursor.StateChangeOrder))
		} else {
			queryBuilder.WriteString(fmt.Sprintf(`
				AND (to_id > %d OR (to_id = %d AND state_change_order > %d))
			`, cursor.ToID, cursor.ToID, cursor.StateChangeOrder))
		}
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY to_id DESC, state_change_order DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY to_id ASC, state_change_order ASC")
	}

	if limit != nil && *limit > 0 {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT $%d", argIndex))
		args = append(args, *limit)
	}

	query := queryBuilder.String()

	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY to_id ASC, state_change_order ASC`, query)
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
// For operation-related state changes, to_id equals the operation ID (when to_id & 4095 != 0).
func (m *StateChangeModel) BatchGetByOperationIDs(ctx context.Context, operationIDs []int64, columns string, limit *int32, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "state_change_order")
	var queryBuilder strings.Builder
	// This CTE query implements per-operation pagination to ensure balanced results.
	// Instead of applying a global LIMIT that could return all state changes from just a few
	// operations, we use ROW_NUMBER() with PARTITION BY to_id to limit results per operation.
	// For operation-related state changes, to_id IS the operation_id.
	queryBuilder.WriteString(fmt.Sprintf(`
		WITH
			inputs (operation_id) AS (
				SELECT * FROM UNNEST($1::bigint[])
			),

			ranked_state_changes_per_operation_id AS (
				SELECT
					sc.*,
					sc.to_id as operation_id,
					ROW_NUMBER() OVER (PARTITION BY sc.to_id ORDER BY sc.to_id %s, sc.state_change_order %s) AS rn
				FROM
					state_changes sc
				JOIN
					inputs i ON sc.to_id = i.operation_id
			)
		SELECT %s, to_id as "cursor.cursor_to_id", state_change_order as "cursor.cursor_state_change_order", operation_id FROM ranked_state_changes_per_operation_id
	`, sortOrder, sortOrder, columns))
	if limit != nil {
		queryBuilder.WriteString(fmt.Sprintf(" WHERE rn <= %d", *limit))
	}
	query := queryBuilder.String()
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY to_id ASC, state_change_order ASC`, query)
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
