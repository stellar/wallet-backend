// memo_migration_test.go tests that `data-migrate run transactions-memo` restores memos
// on history ingested before the memo columns existed.
package integrationtests

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

const memoMigrationName = "transactions-memo"

// MemoMigrationTestSuite rewinds the database to its pre-memo shape (memo columns
// NULL, no data_migrations row) and checks the operator sequence: the command refuses
// until the ingester has recorded the migration, then fills every row back in, byte
// for byte, while live ingestion runs.
type MemoMigrationTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

type storedMemo struct {
	MemoType *int16
	Memo     []byte
}

func (s *MemoMigrationTestSuite) TestMemoMigrationRestoresClearedMemos() {
	ctx := context.Background()
	containers := s.testEnv.Containers

	dbURL, err := containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)
	pool, err := db.OpenDBConnectionPool(ctx, dbURL)
	s.Require().NoError(err)
	defer pool.Close()
	migrations := &data.DataMigrationModel{DB: pool}

	// Stop ingestion so no ledger lands between the snapshot and the clear. If the
	// test fails before the restart, resume it so later suites still have live ingest.
	s.Require().NoError(containers.StopIngestContainer(ctx))
	restarted := false
	defer func() {
		if !restarted {
			if startErr := containers.StartIngestContainer(ctx); startErr != nil {
				log.Ctx(ctx).Errorf("resuming ingest container after failed memo migration test: %v", startErr)
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
	_, err = pool.Exec(ctx, `DELETE FROM data_migrations WHERE name = $1`, memoMigrationName)
	s.Require().NoError(err, "removing the data migration row")

	paymentUseCase := infrastructure.FindUseCase(s.testEnv.UseCases, "Stellarclassic/paymentOp")
	s.Require().NotNil(paymentUseCase, "paymentOp use case not found")
	paymentHash := paymentUseCase.GetTransactionResult.Hash
	cleared, err := s.testEnv.WBClient.GetTransactionByHash(ctx, paymentHash)
	s.Require().NoError(err)
	s.Require().Nil(cleared.MemoType, "memoType should be null while the migration is pending")
	s.Require().Nil(cleared.Memo, "memo should be null while the migration is pending")

	// Before the ingester has recorded the migration its boundary is unknown.
	exitCode, logs, err := containers.RunWalletBackendCommand(ctx, "wallet-backend-data-migrate-unrecorded", "data-migrate run "+memoMigrationName, nil)
	s.Require().NoError(err)
	s.Require().NotZerof(exitCode, "data-migrate run should refuse before the ingester records the migration; logs:\n%s", logs)

	s.Require().NoError(containers.RestartIngestContainer(ctx, nil))
	restarted = true
	s.Require().NoError(containers.WaitForIngestCatchup(ctx), "ingest did not catch up after restart")

	row, err := migrations.Get(ctx, memoMigrationName)
	s.Require().NoError(err)
	s.Require().NotNil(row, "the ingester should record the migration at startup")
	s.Require().Equal(data.DataMigrationPending, row.Status)

	exitCode, logs, err = containers.RunWalletBackendCommand(ctx, "wallet-backend-data-migrate", "data-migrate run "+memoMigrationName, nil)
	s.Require().NoError(err)
	s.Require().Zerof(exitCode, "data-migrate run should exit 0 (see `docker logs wallet-backend-data-migrate`); logs:\n%s", logs)

	row, err = migrations.Get(ctx, memoMigrationName)
	s.Require().NoError(err)
	s.Require().Equal(data.DataMigrationDone, row.Status)

	after := s.readMemos(ctx, pool)
	for toID, want := range before {
		got, ok := after[toID]
		s.Require().True(ok, "transaction %d disappeared during the memo migration", toID)
		s.Require().Equal(want, got, "transaction %d memo differs after the migration", toID)
	}

	// The API serves the restored memo again.
	tx, err := s.testEnv.WBClient.GetTransactionByHash(ctx, paymentHash)
	s.Require().NoError(err)
	s.Require().NotNil(tx.MemoType, "memoType should be set after the migration")
	s.Require().Equal(types.MemoTypeText, *tx.MemoType)
	s.Require().NotNil(tx.Memo)
	s.Require().Equal(infrastructure.PaymentOpMemoText, *tx.Memo)
}

// readMemos returns every transactions row's stored memo keyed by to_id.
func (s *MemoMigrationTestSuite) readMemos(ctx context.Context, pool *pgxpool.Pool) map[int64]storedMemo {
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
