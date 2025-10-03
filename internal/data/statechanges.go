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

type StateChangeModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

// BatchGetByAccountAddress gets the state changes that are associated with the given account addresses.
func (m *StateChangeModel) BatchGetByAccountAddress(ctx context.Context, accountAddress string, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "state_change_order")
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, to_id as "cursor.cursor_to_id", state_change_order as "cursor.cursor_state_change_order"
		FROM state_changes 
		WHERE account_id = $1
	`, columns))

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
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT %d", *limit))
	}

	query := queryBuilder.String()

	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS statechanges ORDER BY to_id ASC, state_change_order ASC`, query)
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
	offerIDs := make([]*string, len(stateChanges))
	signerAccountIDs := make([]*string, len(stateChanges))
	spenderAccountIDs := make([]*string, len(stateChanges))
	sponsoredAccountIDs := make([]*string, len(stateChanges))
	sponsorAccountIDs := make([]*string, len(stateChanges))
	deployerAccountIDs := make([]*string, len(stateChanges))
	funderAccountIDs := make([]*string, len(stateChanges))
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
		if sc.DeployerAccountID.Valid {
			deployerAccountIDs[i] = &sc.DeployerAccountID.String
		}
		if sc.FunderAccountID.Valid {
			funderAccountIDs[i] = &sc.FunderAccountID.String
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
		-- Insert state changes
		WITH input_data AS (
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
				UNNEST($12::text[]) AS offer_id,
				UNNEST($13::text[]) AS signer_account_id,
				UNNEST($14::text[]) AS spender_account_id,
				UNNEST($15::text[]) AS sponsored_account_id,
				UNNEST($16::text[]) AS sponsor_account_id,
				UNNEST($17::text[]) AS deployer_account_id,
				UNNEST($18::text[]) AS funder_account_id,
				UNNEST($19::jsonb[]) AS signer_weights,
				UNNEST($20::jsonb[]) AS thresholds,
				UNNEST($21::jsonb[]) AS flags,
				UNNEST($22::jsonb[]) AS key_value
		),
		inserted_state_changes AS (
			INSERT INTO state_changes
				(state_change_order, to_id, state_change_category, state_change_reason, ledger_created_at,
				ledger_number, account_id, operation_id, tx_hash, token_id, amount,
				offer_id, signer_account_id,
				spender_account_id, sponsored_account_id, sponsor_account_id,
				deployer_account_id, funder_account_id, signer_weights, thresholds, flags, key_value)
			SELECT
				sc.state_change_order, sc.to_id, sc.state_change_category, sc.state_change_reason, sc.ledger_created_at,
				sc.ledger_number, sc.account_id, sc.operation_id, sc.tx_hash, sc.token_id, sc.amount,
				sc.offer_id, sc.signer_account_id,
				sc.spender_account_id, sc.sponsored_account_id, sc.sponsor_account_id,
				sc.deployer_account_id, sc.funder_account_id, sc.signer_weights, sc.thresholds, sc.flags, sc.key_value
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
		pq.Array(ledgerNumbers),
		pq.Array(accountIDs),
		pq.Array(operationIDs),
		pq.Array(txHashes),
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

// BatchGetByTxHash gets state changes for a single transaction with pagination support.
func (m *StateChangeModel) BatchGetByTxHash(ctx context.Context, txHash string, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "state_change_order")
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, to_id as "cursor.cursor_to_id", state_change_order as "cursor.cursor_state_change_order"
		FROM state_changes 
		WHERE tx_hash = $1
	`, columns))

	args := []interface{}{txHash}
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
	m.MetricsService.ObserveDBQueryDuration("SELECT", "state_changes", duration)
	if err != nil {
		return nil, fmt.Errorf("getting paginated state changes by tx hash: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "state_changes")
	return stateChanges, nil
}

// BatchGetByTxHashes gets the state changes that are associated with the given transaction hashes.
func (m *StateChangeModel) BatchGetByTxHashes(ctx context.Context, txHashes []string, columns string, limit *int32, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "state_change_order")
	var queryBuilder strings.Builder
	// This CTE query implements per-transaction pagination to ensure balanced results.
	// Instead of applying a global LIMIT that could return all state changes from just a few
	// transactions, we use ROW_NUMBER() with PARTITION BY tx_hash to limit results per transaction.
	// This guarantees that each transaction gets at most 'limit' state changes, providing
	// more balanced and predictable pagination across multiple transactions.
	queryBuilder.WriteString(fmt.Sprintf(`
		WITH
			inputs (tx_hash) AS (
				SELECT * FROM UNNEST($1::text[])
			),
			
			ranked_state_changes_per_tx_hash AS (
				SELECT
					sc.*,
					ROW_NUMBER() OVER (PARTITION BY sc.tx_hash ORDER BY sc.to_id %s, sc.state_change_order %s) AS rn
				FROM 
					state_changes sc
				JOIN 
					inputs i ON sc.tx_hash = i.tx_hash
			)
		SELECT %s, to_id as "cursor.cursor_to_id", state_change_order as "cursor.cursor_state_change_order" FROM ranked_state_changes_per_tx_hash
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
	m.MetricsService.ObserveDBQueryDuration("SELECT", "state_changes", duration)
	if err != nil {
		return nil, fmt.Errorf("getting state changes by transaction hashes: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "state_changes")
	return stateChanges, nil
}

// BatchGetByOperationID gets state changes for a single operation with pagination support.
func (m *StateChangeModel) BatchGetByOperationID(ctx context.Context, operationID int64, columns string, limit *int32, cursor *types.StateChangeCursor, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "state_change_order")
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, to_id as "cursor.cursor_to_id", state_change_order as "cursor.cursor_state_change_order"
		FROM state_changes 
		WHERE operation_id = $1
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
	m.MetricsService.ObserveDBQueryDuration("SELECT", "state_changes", duration)
	if err != nil {
		return nil, fmt.Errorf("getting paginated state changes by operation ID: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "state_changes")
	return stateChanges, nil
}

// BatchGetByOperationIDs gets the state changes that are associated with the given operation IDs.
func (m *StateChangeModel) BatchGetByOperationIDs(ctx context.Context, operationIDs []int64, columns string, limit *int32, sortOrder SortOrder) ([]*types.StateChangeWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.StateChange{}, "", "to_id", "state_change_order")
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
					ROW_NUMBER() OVER (PARTITION BY sc.operation_id ORDER BY sc.to_id %s, sc.state_change_order %s) AS rn
				FROM 
					state_changes sc
				JOIN 
					inputs i ON sc.operation_id = i.operation_id
			)
		SELECT %s, to_id as "cursor.cursor_to_id", state_change_order as "cursor.cursor_state_change_order" FROM ranked_state_changes_per_operation_id
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
	m.MetricsService.ObserveDBQueryDuration("SELECT", "state_changes", duration)
	if err != nil {
		return nil, fmt.Errorf("getting state changes by operation IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "state_changes")
	return stateChanges, nil
}
