package datamigrations

import (
	"context"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/utils"
)

// transactionsMemo fills transactions.memo_type/memo for rows ingested before the memo
// columns existed, from each transaction's envelope.
type transactionsMemo struct{}

func (transactionsMemo) Name() string { return "transactions-memo" }

func (transactionsMemo) Tables() []string { return []string{"transactions"} }

func (transactionsMemo) Extract(ctx context.Context, deps Deps, ledger xdr.LedgerCloseMeta) ([]any, error) {
	seq := ledger.LedgerSequence()
	// toid packs the ledger into the high 32 bits of a signed int64, so a ledger above
	// MaxInt32 would wrap negative and match no row. Mainnet is four orders of magnitude
	// below this; fail loudly instead of silently skipping.
	if seq > math.MaxInt32 {
		return nil, fmt.Errorf("ledger %d exceeds the largest toid ledger %d", seq, int64(math.MaxInt32))
	}
	txs, err := indexer.GetLedgerTransactions(ctx, deps.NetworkPassphrase, ledger, deps.Pool)
	if err != nil {
		return nil, fmt.Errorf("reading ledger %d transactions: %w", seq, err)
	}
	closedAt := ledger.ClosedAt()
	rows := make([]any, 0, len(txs))
	for _, tx := range txs {
		memoType, memo := utils.EncodeMemo(tx.Envelope.Memo())
		rows = append(rows, data.TransactionMemo{
			ToID:            toid.New(int32(seq), int32(tx.Index), 0).ToInt64(),
			LedgerCreatedAt: closedAt,
			MemoType:        memoType,
			Memo:            memo,
		})
	}
	return rows, nil
}

func (transactionsMemo) Apply(ctx context.Context, deps Deps, dbTx pgx.Tx, rows []any) error {
	memos := make([]data.TransactionMemo, len(rows))
	for i, r := range rows {
		memos[i] = r.(data.TransactionMemo) //nolint:errcheck // Extract returns only this type
	}
	if _, err := deps.Models.Transactions.BatchUpdateMemo(ctx, dbTx, memos); err != nil {
		return fmt.Errorf("updating memos: %w", err)
	}
	return nil
}
