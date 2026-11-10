// memo_backfill_test.go tests that the live ingester's memo backfill restores memos
// on history ingested before the memo columns existed.
package integrationtests

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

const memoBackfillTimeout = 5 * time.Minute

// MemoBackfillTestSuite rewinds the database to its pre-memo shape (memo columns
// NULL, backfill cursor unset) and checks that restarting the ingester fills every
// row back in, byte for byte.
type MemoBackfillTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

type storedMemo struct {
	MemoType *int16
	Memo     []byte
}

func (s *MemoBackfillTestSuite) TestMemoBackfillRestoresClearedMemos() {
	ctx := context.Background()
	containers := s.testEnv.Containers

	dbURL, err := containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)
	pool, err := db.OpenDBConnectionPool(ctx, dbURL)
	s.Require().NoError(err)
	defer pool.Close()

	// Stop ingestion so no ledger lands between the snapshot and the clear. If the
	// test fails before the restart, resume it so later suites still have live ingest.
	s.Require().NoError(containers.StopIngestContainer(ctx))
	restarted := false
	defer func() {
		if !restarted {
			if startErr := containers.StartIngestContainer(ctx); startErr != nil {
				log.Ctx(ctx).Errorf("resuming ingest container after failed memo backfill test: %v", startErr)
			}
		}
	}()

	before := s.readMemos(ctx, pool)
	s.Require().NotEmpty(before, "transactions table should hold the ingested fixtures")
	for toID, m := range before {
		s.Require().NotNil(m.MemoType, "transaction %d was ingested without a memo before the clear", toID)
	}

	_, err = pool.Exec(ctx, `UPDATE transactions SET memo_type = NULL, memo = NULL`)
	s.Require().NoError(err, "clearing transaction memos")
	_, err = pool.Exec(ctx, `
		INSERT INTO ingest_store (key, value) VALUES ($1, '0')
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`, data.MemoBackfillCursorName)
	s.Require().NoError(err, "resetting the memo backfill cursor")

	paymentUseCase := infrastructure.FindUseCase(s.testEnv.UseCases, "Stellarclassic/paymentOp")
	s.Require().NotNil(paymentUseCase, "paymentOp use case not found")
	paymentHash := paymentUseCase.GetTransactionResult.Hash
	cleared, err := s.testEnv.WBClient.GetTransactionByHash(ctx, paymentHash)
	s.Require().NoError(err)
	s.Require().Nil(cleared.MemoType, "memoType should be null while the backfill is pending")
	s.Require().Nil(cleared.Memo, "memo should be null while the backfill is pending")

	s.Require().NoError(containers.RestartIngestContainer(ctx, nil))
	restarted = true
	s.Require().NoError(containers.WaitForIngestCatchup(ctx), "ingest did not catch up after restart")

	s.Require().NoError(s.waitForMemoBackfill(ctx), "memo backfill did not complete")

	after := s.readMemos(ctx, pool)
	for toID, want := range before {
		got, ok := after[toID]
		s.Require().True(ok, "transaction %d disappeared during the memo backfill", toID)
		s.Require().Equal(want, got, "transaction %d memo differs after the backfill", toID)
	}

	// The API serves the backfilled memo again.
	tx, err := s.testEnv.WBClient.GetTransactionByHash(ctx, paymentHash)
	s.Require().NoError(err)
	s.Require().NotNil(tx.MemoType, "memoType should be set after the backfill")
	s.Require().Equal(types.MemoTypeText, *tx.MemoType)
	s.Require().NotNil(tx.Memo)
	s.Require().Equal(infrastructure.PaymentOpMemoText, *tx.Memo)
}

// readMemos returns every transactions row's stored memo keyed by to_id.
func (s *MemoBackfillTestSuite) readMemos(ctx context.Context, pool *pgxpool.Pool) map[int64]storedMemo {
	rows, err := pool.Query(ctx, `SELECT to_id, memo_type, memo FROM transactions`)
	s.Require().NoError(err, "querying transaction memos")
	defer rows.Close()

	memos := make(map[int64]storedMemo)
	for rows.Next() {
		var toID int64
		var m storedMemo
		s.Require().NoError(rows.Scan(&toID, &m.MemoType, &m.Memo))
		memos[toID] = m
	}
	s.Require().NoError(rows.Err())
	return memos
}

// waitForMemoBackfill polls until the memo backfill cursor is set and at or below
// oldest_ingest_ledger. The cursor starts at 0 after the reset, which the ingester
// replaces with latest+1 on start, so 0 never counts as done.
func (s *MemoBackfillTestSuite) waitForMemoBackfill(ctx context.Context) error {
	containers := s.testEnv.Containers
	deadline := time.Now().Add(memoBackfillTimeout)
	var cursor, oldest uint32
	for time.Now().Before(deadline) {
		var err error
		cursor, err = containers.GetIngestCursor(ctx, data.MemoBackfillCursorName)
		if err != nil {
			return fmt.Errorf("reading memo backfill cursor: %w", err)
		}
		oldest, err = containers.GetIngestCursor(ctx, data.OldestLedgerCursorName)
		if err != nil {
			return fmt.Errorf("reading oldest ingest ledger: %w", err)
		}
		log.Ctx(ctx).Infof("Memo backfill progress: cursor=%d, oldest_ingest_ledger=%d", cursor, oldest)
		if cursor != 0 && cursor <= oldest {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("memo backfill cursor=%d still above oldest_ingest_ledger=%d after %s", cursor, oldest, memoBackfillTimeout)
}
