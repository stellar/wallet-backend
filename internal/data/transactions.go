package data

import (
	"context"
	"fmt"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

type TransactionModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

// BatchInsert inserts the transactions and the transactions_accounts links.
// It returns the hashes of the successfully inserted transactions.
func (m *TransactionModel) BatchInsert(
	ctx context.Context,
	sqlExecuter db.SQLExecuter,
	txs []types.Transaction,
	stellarAddressesByTxHash map[string]set.Set[string],
) ([]string, error) {
	if sqlExecuter == nil {
		sqlExecuter = m.DB
	}

	// 1. Flatten the transactions into parallel slices
	hashes := make([]string, len(txs))
	toIDs := make([]int64, len(txs))
	envelopeXDRs := make([]string, len(txs))
	resultXDRs := make([]string, len(txs))
	metaXDRs := make([]string, len(txs))
	ledgerNumbers := make([]int, len(txs))
	ledgerCreatedAts := make([]time.Time, len(txs))

	for i, t := range txs {
		hashes[i] = t.Hash
		toIDs[i] = t.ToID
		envelopeXDRs[i] = t.EnvelopeXDR
		resultXDRs[i] = t.ResultXDR
		metaXDRs[i] = t.MetaXDR
		ledgerNumbers[i] = int(t.LedgerNumber)
		ledgerCreatedAts[i] = t.LedgerCreatedAt
	}

	// 2. Flatten the stellarAddressesByTxHash into parallel slices
	var txHashes, stellarAddresses []string
	for txHash, addresses := range stellarAddressesByTxHash {
		for address := range addresses.Iter() {
			txHashes = append(txHashes, txHash)
			stellarAddresses = append(stellarAddresses, address)
		}
	}

	// 3. Single query that inserts only transactions that are connected to at least one existing account.
	// It also inserts the transactions_accounts links.
	const insertQuery = `
	-- STEP 1: Get existing accounts
	WITH existing_accounts AS (
		SELECT stellar_address FROM accounts WHERE stellar_address=ANY($9)
	),

	-- STEP 2: Get transaction hashes to insert (connected to at least one existing account)
    valid_transactions AS (
        SELECT DISTINCT tx_hash
        FROM (
			SELECT
				UNNEST($8::text[]) AS tx_hash,
				UNNEST($9::text[]) AS account_id
		) ta
		WHERE ta.account_id IN (SELECT stellar_address FROM existing_accounts)
    ),

	-- STEP 3: Insert those transactions
    inserted_transactions AS (
        INSERT INTO transactions
          	(hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
        SELECT
            vt.tx_hash,
            t.to_id,
            t.envelope_xdr,
            t.result_xdr,
            t.meta_xdr,
            t.ledger_number,
            t.ledger_created_at
        FROM valid_transactions vt
        JOIN (
            SELECT
                UNNEST($1::text[]) AS hash,
                UNNEST($2::bigint[]) AS to_id,
                UNNEST($3::text[]) AS envelope_xdr,
                UNNEST($4::text[]) AS result_xdr,
                UNNEST($5::text[]) AS meta_xdr,
                UNNEST($6::bigint[]) AS ledger_number,
                UNNEST($7::timestamptz[]) AS ledger_created_at
        ) t ON t.hash = vt.tx_hash
        ON CONFLICT (hash) DO NOTHING
        RETURNING hash
    ),

	-- STEP 4: Insert transactions_accounts links
	inserted_transactions_accounts AS (
		INSERT INTO transactions_accounts
			(tx_hash, account_id)
		SELECT
			ta.tx_hash, ta.account_id
		FROM (
			SELECT
				UNNEST($8::text[]) AS tx_hash,
				UNNEST($9::text[]) AS account_id
		) ta
		INNER JOIN existing_accounts ea ON ea.stellar_address = ta.account_id
		INNER JOIN inserted_transactions it ON it.hash = ta.tx_hash
		ON CONFLICT DO NOTHING
	)

	-- STEP 5: Return the hashes of successfully inserted transactions
	SELECT hash FROM inserted_transactions;
    `

	start := time.Now()
	var insertedHashes []string
	err := sqlExecuter.SelectContext(ctx, &insertedHashes, insertQuery,
		pq.Array(hashes),
		pq.Array(toIDs),
		pq.Array(envelopeXDRs),
		pq.Array(resultXDRs),
		pq.Array(metaXDRs),
		pq.Array(ledgerNumbers),
		pq.Array(ledgerCreatedAts),
		pq.Array(txHashes),
		pq.Array(stellarAddresses),
	)
	duration := time.Since(start).Seconds()
	for _, dbTableName := range []string{"transactions", "transactions_accounts"} {
		m.MetricsService.ObserveDBQueryDuration("INSERT", dbTableName, duration)
		if err == nil {
			m.MetricsService.IncDBQuery("INSERT", dbTableName)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("batch inserting transactions and transactions_accounts: %w", err)
	}

	return insertedHashes, nil
}
