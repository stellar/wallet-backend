package data

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

var stateChangeColumns = getDBColumns(types.StateChange{})

type StateChangeCursor struct {
	ToID             int64
	StateChangeOrder int64
}

type StateChangeModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

// BatchGetByAccountAddress gets the state changes that are associated with the given account addresses.
func (m *StateChangeModel) BatchGetByAccountAddress(
	ctx context.Context,
	accountAddress string,
	columns string,
	limit *int32,
	cursor *StateChangeCursor,
) ([]*types.StateChangeWithCursor, error) {
	if columns == "" {
		columns = strings.Join(stateChangeColumns, ", ")
	}

	query := fmt.Sprintf(`
		SELECT %s, CONCAT(to_id, ':', state_change_order) as sc_cursor FROM state_changes WHERE account_id = $1
	`, columns)
	if cursor != nil {
		query += fmt.Sprintf(` AND (to_id < %d OR (to_id = %d AND state_change_order < %d))`, cursor.ToID, cursor.ToID, cursor.StateChangeOrder)
	}
	query += ` ORDER BY to_id DESC, state_change_order DESC`

	if limit != nil && *limit > 0 {
		query += fmt.Sprintf(` LIMIT %d`, *limit)
	}

	var stateChanges []*types.StateChangeWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &stateChanges, query, accountAddress)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "state_changes", duration)
	if err != nil {
		return nil, fmt.Errorf("getting state changes by account address: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "state_changes")
	return stateChanges, nil
}

func (m *StateChangeModel) GetAll(ctx context.Context, columns string, limit *int32, cursor *StateChangeCursor) ([]*types.StateChangeWithCursor, error) {
	if columns == "" {
		columns = strings.Join(stateChangeColumns, ", ")
	}
	query := fmt.Sprintf(`SELECT %s, to_id, state_change_order, CONCAT(to_id, ':', state_change_order) as sc_cursor FROM state_changes`, columns)

	if cursor != nil {
		query += fmt.Sprintf(` WHERE to_id < %d OR (to_id = %d AND state_change_order < %d)`, cursor.ToID, cursor.ToID, cursor.StateChangeOrder)
	}
	query += ` ORDER BY to_id DESC, state_change_order DESC`

	if limit != nil && *limit > 0 {
		query += fmt.Sprintf(` LIMIT %d`, *limit)
	}
	var stateChanges []*types.StateChangeWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &stateChanges, query)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "state_changes", duration)
	if err != nil {
		return nil, fmt.Errorf("getting all state changes: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "state_changes")
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
	accountIDs := make([]string, len(stateChanges))
	operationIDs := make([]int64, len(stateChanges))
	txHashes := make([]string, len(stateChanges))
	tokenIDs := make([]*string, len(stateChanges))
	amounts := make([]*string, len(stateChanges))
	claimableBalanceIDs := make([]*string, len(stateChanges))
	liquidityPoolIDs := make([]*string, len(stateChanges))
	offerIDs := make([]*string, len(stateChanges))
	signerAccountIDs := make([]*string, len(stateChanges))
	spenderAccountIDs := make([]*string, len(stateChanges))
	sponsoredAccountIDs := make([]*string, len(stateChanges))
	sponsorAccountIDs := make([]*string, len(stateChanges))
	signerWeights := make([]*types.NullableJSONB, len(stateChanges))
	thresholds := make([]*types.NullableJSONB, len(stateChanges))
	flags := make([]*types.NullableJSON, len(stateChanges))
	keyValues := make([]*types.NullableJSONB, len(stateChanges))

	for i, sc := range stateChanges {
		stateChangeOrders[i] = sc.StateChangeOrder
		toIDs[i] = sc.ToID
		categories[i] = string(sc.StateChangeCategory)
		ledgerCreatedAts[i] = sc.LedgerCreatedAt
		ledgerNumbers[i] = int(sc.LedgerNumber)
		accountIDs[i] = sc.AccountID
		operationIDs[i] = sc.OperationID
		txHashes[i] = sc.TxHash

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
		if sc.ClaimableBalanceID.Valid {
			claimableBalanceIDs[i] = &sc.ClaimableBalanceID.String
		}
		if sc.LiquidityPoolID.Valid {
			liquidityPoolIDs[i] = &sc.LiquidityPoolID.String
		}
		if sc.OfferID.Valid {
			offerIDs[i] = &sc.OfferID.String
		}
		if sc.SignerAccountID.Valid {
			signerAccountIDs[i] = &sc.SignerAccountID.String
		}
		if sc.SpenderAccountID.Valid {
			spenderAccountIDs[i] = &sc.SpenderAccountID.String
		}
		if sc.SponsoredAccountID.Valid {
			sponsoredAccountIDs[i] = &sc.SponsoredAccountID.String
		}
		if sc.SponsorAccountID.Valid {
			sponsorAccountIDs[i] = &sc.SponsorAccountID.String
		}
		if sc.SignerWeights != nil {
			signerWeights[i] = &sc.SignerWeights
		}
		if sc.Thresholds != nil {
			thresholds[i] = &sc.Thresholds
		}
		if sc.Flags != nil {
			flags[i] = &sc.Flags
		}
		if sc.KeyValue != nil {
			keyValues[i] = &sc.KeyValue
		}
	}

	const insertQuery = `
		-- STEP 1: Get existing accounts
		WITH existing_accounts AS (
			SELECT stellar_address FROM accounts WHERE stellar_address=ANY($7)
		),

		-- STEP 2: Create properly aligned data from arrays
		input_data AS (
			SELECT
				UNNEST($1::bigint[]) AS state_change_order,
				UNNEST($2::bigint[]) AS to_id,
				UNNEST($3::text[]) AS state_change_category,
				UNNEST($4::text[]) AS state_change_reason,
				UNNEST($5::timestamptz[]) AS ledger_created_at,
				UNNEST($6::integer[]) AS ledger_number,
				UNNEST($7::text[]) AS account_id,
				UNNEST($8::bigint[]) AS operation_id,
				UNNEST($9::text[]) AS tx_hash,
				UNNEST($10::text[]) AS token_id,
				UNNEST($11::text[]) AS amount,
				UNNEST($12::text[]) AS claimable_balance_id,
				UNNEST($13::text[]) AS liquidity_pool_id,
				UNNEST($14::text[]) AS offer_id,
				UNNEST($15::text[]) AS signer_account_id,
				UNNEST($16::text[]) AS spender_account_id,
				UNNEST($17::text[]) AS sponsored_account_id,
				UNNEST($18::text[]) AS sponsor_account_id,
				UNNEST($19::jsonb[]) AS signer_weights,
				UNNEST($20::jsonb[]) AS thresholds,
				UNNEST($21::jsonb[]) AS flags,
				UNNEST($22::jsonb[]) AS key_value
		),

		-- STEP 3: Get state changes that reference existing accounts
		valid_state_changes AS (
			SELECT sc.*
			FROM input_data sc
			WHERE sc.account_id IN (SELECT stellar_address FROM existing_accounts)
		),

		-- STEP 4: Insert the valid state changes
		inserted_state_changes AS (
			INSERT INTO state_changes
				(state_change_order, to_id, state_change_category, state_change_reason, ledger_created_at,
				ledger_number, account_id, operation_id, tx_hash, token_id, amount,
				claimable_balance_id, liquidity_pool_id, offer_id, signer_account_id,
				spender_account_id, sponsored_account_id, sponsor_account_id,
				signer_weights, thresholds, flags, key_value)
			SELECT
				state_change_order, to_id, state_change_category, state_change_reason, ledger_created_at,
				ledger_number, account_id, operation_id, tx_hash, token_id, amount,
				claimable_balance_id, liquidity_pool_id, offer_id, signer_account_id,
				spender_account_id, sponsored_account_id, sponsor_account_id,
				signer_weights, thresholds, flags, key_value
			FROM valid_state_changes
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
		pq.Array(ledgerNumbers),
		pq.Array(accountIDs),
		pq.Array(operationIDs),
		pq.Array(txHashes),
		pq.Array(tokenIDs),
		pq.Array(amounts),
		pq.Array(claimableBalanceIDs),
		pq.Array(liquidityPoolIDs),
		pq.Array(offerIDs),
		pq.Array(signerAccountIDs),
		pq.Array(spenderAccountIDs),
		pq.Array(sponsoredAccountIDs),
		pq.Array(sponsorAccountIDs),
		pq.Array(signerWeights),
		pq.Array(thresholds),
		pq.Array(flags),
		pq.Array(keyValues),
	)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("INSERT", "state_changes", duration)
	if err != nil {
		return nil, fmt.Errorf("batch inserting state changes: %w", err)
	}
	m.MetricsService.IncDBQuery("INSERT", "state_changes")

	return insertedIDs, nil
}

// BatchGetByTxHashes gets the state changes that are associated with the given transaction hashes.
func (m *StateChangeModel) BatchGetByTxHashes(ctx context.Context, txHashes []string, columns string, limit *int32, cursors []*StateChangeCursor) ([]*types.StateChangeWithCursor, error) {
	if columns == "" {
		columns = strings.Join(stateChangeColumns, ", ")
	}

	toIDs := make([]*int64, len(cursors))
	stateChangeOrders := make([]*int64, len(cursors))
	for i, cursor := range cursors {
		if cursor == nil {
			continue
		}
		toIDs[i] = &cursor.ToID
		stateChangeOrders[i] = &cursor.StateChangeOrder
	}

	query := `
		WITH
			inputs (tx_hash, to_id, state_change_order) AS (
				SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::bigint[])
			),
			
			ranked_state_changes_per_tx_hash AS (
				SELECT
					sc.*,
					ROW_NUMBER() OVER (PARTITION BY sc.tx_hash ORDER BY sc.to_id DESC, sc.state_change_order DESC) AS rn
				FROM 
					state_changes sc
				JOIN 
					inputs i ON sc.tx_hash = i.tx_hash
				WHERE 
					((i.to_id IS NULL AND i.state_change_order IS NULL) OR sc.to_id < i.to_id OR (sc.to_id = i.to_id AND sc.state_change_order < i.state_change_order))
			)
		SELECT %s, tx_hash, CONCAT(to_id, ':', state_change_order) as sc_cursor FROM ranked_state_changes_per_tx_hash
	`
	query = fmt.Sprintf(query, columns)

	if limit != nil && *limit > 0 {
		query += fmt.Sprintf(` WHERE rn <= %d`, *limit)
	}

	var stateChanges []*types.StateChangeWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &stateChanges, query, pq.Array(txHashes), pq.Array(toIDs), pq.Array(stateChangeOrders))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "state_changes", duration)
	if err != nil {
		return nil, fmt.Errorf("getting state changes by transaction hashes: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "state_changes")
	return stateChanges, nil
}

// BatchGetByOperationIDs gets the state changes that are associated with the given operation IDs.
func (m *StateChangeModel) BatchGetByOperationIDs(ctx context.Context, operationIDs []int64, columns string) ([]*types.StateChange, error) {
	if columns == "" {
		columns = "*"
	}
	query := fmt.Sprintf(`
		SELECT %s, operation_id FROM state_changes WHERE operation_id = ANY($1)
	`, columns)
	var stateChanges []*types.StateChange
	start := time.Now()
	err := m.DB.SelectContext(ctx, &stateChanges, query, pq.Array(operationIDs))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "state_changes", duration)
	if err != nil {
		return nil, fmt.Errorf("getting state changes by operation IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "state_changes")
	return stateChanges, nil
}
