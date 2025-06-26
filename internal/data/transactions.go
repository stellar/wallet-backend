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

type TransactionModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

func (m *TransactionModel) BatchInsert(
	ctx context.Context,
	dbTx db.SQLExecuter,
	txs []types.Transaction,
	stellarAddressesByTxHash map[string][]string,
) error {
	now := time.Now()
	var sqlExecuter db.SQLExecuter = dbTx
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
	ingestedAts := make([]time.Time, len(txs))

	for i, t := range txs {
		hashes[i] = t.Hash
		toIDs[i] = t.ToID
		envelopeXDRs[i] = t.EnvelopeXDR
		resultXDRs[i] = t.ResultXDR
		metaXDRs[i] = t.MetaXDR
		ledgerNumbers[i] = int(t.LedgerNumber)
		ledgerCreatedAts[i] = t.LedgerCreatedAt
		ingestedAts[i] = now
	}

	// 2. Batch insert the transactions into the database
	const insertTxsQuery = `
    INSERT INTO transactions
      (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at, ingested_at)
    SELECT
      UNNEST($1::text[]),
      UNNEST($2::bigint[]),
      UNNEST($3::text[]),
      UNNEST($4::text[]),
      UNNEST($5::text[]),
      UNNEST($6::bigint[]),
      UNNEST($7::timestamptz[]),
      UNNEST($8::timestamptz[])
    ON CONFLICT (hash) DO NOTHING;
    `

	start := time.Now()
	_, err := sqlExecuter.ExecContext(ctx, insertTxsQuery,
		pq.Array(hashes),
		pq.Array(toIDs),
		pq.Array(envelopeXDRs),
		pq.Array(resultXDRs),
		pq.Array(metaXDRs),
		pq.Array(ledgerNumbers),
		pq.Array(ledgerCreatedAts),
		pq.Array(ingestedAts),
	)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("INSERT", "transactions", duration)
	if err != nil {
		return fmt.Errorf("batch inserting transactions: %w", err)
	}
	m.MetricsService.IncDBQuery("INSERT", "transactions")

	// 3. Flatten the stellarAddressesByTxHash into parallel slices
	var (
		txHashes   []string
		accountIDs []string
	)
	for txHash, addrs := range stellarAddressesByTxHash {
		for _, acct := range addrs {
			txHashes = append(txHashes, txHash)
			accountIDs = append(accountIDs, acct)
		}
	}

	// 4. Batch insert the transactions_accounts into the database
	const insertLinks = `
    INSERT INTO transactions_accounts (tx_hash, account_id)
    SELECT
      UNNEST($1::text[]),
      UNNEST($2::text[])
    ON CONFLICT DO NOTHING;
    `
	start = time.Now()
	_, err = sqlExecuter.ExecContext(ctx, insertLinks,
		pq.Array(txHashes),
		pq.Array(accountIDs),
	)
	duration = time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("INSERT", "transactions_accounts", duration)
	if err != nil {
		return fmt.Errorf("batch insert transactions_accounts: %w", err)
	}
	m.MetricsService.IncDBQuery("INSERT", "transactions_accounts")

	return nil
}
