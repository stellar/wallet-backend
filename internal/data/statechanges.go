package data

import (
	"context"
	"fmt"
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

func (m *StateChangeModel) BatchInsert(
	ctx context.Context,
	sqlExecuter db.SQLExecuter,
	stateChanges []types.StateChange,
) ([]string, error) {
	if sqlExecuter == nil {
		sqlExecuter = m.DB
	}

	// Flatten the state changes into parallel slices
	ids := make([]string, len(stateChanges))
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
		ids[i] = sc.ID
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
			SELECT stellar_address FROM accounts WHERE stellar_address=ANY($6)
		),

		-- STEP 2: Create properly aligned data from arrays
		input_data AS (
			SELECT
				UNNEST($1::text[]) AS id,
				UNNEST($2::text[]) AS state_change_category,
				UNNEST($3::text[]) AS state_change_reason,
				UNNEST($4::timestamptz[]) AS ledger_created_at,
				UNNEST($5::integer[]) AS ledger_number,
				UNNEST($6::text[]) AS account_id,
				UNNEST($7::bigint[]) AS operation_id,
				UNNEST($8::text[]) AS tx_hash,
				UNNEST($9::text[]) AS token_id,
				UNNEST($10::text[]) AS amount,
				UNNEST($11::text[]) AS claimable_balance_id,
				UNNEST($12::text[]) AS liquidity_pool_id,
				UNNEST($13::text[]) AS offer_id,
				UNNEST($14::text[]) AS signer_account_id,
				UNNEST($15::text[]) AS spender_account_id,
				UNNEST($16::text[]) AS sponsored_account_id,
				UNNEST($17::text[]) AS sponsor_account_id,
				UNNEST($18::jsonb[]) AS signer_weights,
				UNNEST($19::jsonb[]) AS thresholds,
				UNNEST($20::jsonb[]) AS flags,
				UNNEST($21::jsonb[]) AS key_value
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
				(id, state_change_category, state_change_reason, ledger_created_at,
				ledger_number, account_id, operation_id, tx_hash, token_id, amount,
				claimable_balance_id, liquidity_pool_id, offer_id, signer_account_id,
				spender_account_id, sponsored_account_id, sponsor_account_id,
				signer_weights, thresholds, flags, key_value)
			SELECT
				id, state_change_category, state_change_reason, ledger_created_at,
				ledger_number, account_id, operation_id, tx_hash, token_id, amount,
				claimable_balance_id, liquidity_pool_id, offer_id, signer_account_id,
				spender_account_id, sponsored_account_id, sponsor_account_id,
				signer_weights, thresholds, flags, key_value
			FROM valid_state_changes
			ON CONFLICT (id) DO NOTHING
			RETURNING id
		)

		SELECT id FROM inserted_state_changes;
	`

	start := time.Now()
	var insertedIDs []string
	err := sqlExecuter.SelectContext(ctx, &insertedIDs, insertQuery,
		pq.Array(ids),
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
	if err == nil {
		m.MetricsService.IncDBQuery("INSERT", "state_changes")
	}
	if err != nil {
		return nil, fmt.Errorf("batch inserting state changes: %w", err)
	}

	return insertedIDs, nil
}