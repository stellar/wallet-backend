package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/utils"
)

func createChannelAccountFixture(t *testing.T, ctx context.Context, dbConnectionPool *pgxpool.Pool, channelAccounts ...ChannelAccount) {
	t.Helper()
	if len(channelAccounts) == 0 {
		return
	}
	const q = `
		INSERT INTO
			channel_accounts (public_key, encrypted_private_key, locked_tx_hash, locked_at, locked_until)
		VALUES
			($1, $2, $3, $4, $5)
	`
	for _, ca := range channelAccounts {
		_, err := dbConnectionPool.Exec(ctx, q, ca.PublicKey, ca.EncryptedPrivateKey, ca.LockedTxHash, ca.LockedAt, ca.LockedUntil)
		require.NoError(t, err)
	}
}

func TestChannelAccountModelGetAndLockIdleChannelAccount(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := NewChannelAccountModel(dbConnectionPool)

	kp1 := keypair.MustRandom()
	kp2 := keypair.MustRandom()
	channelAccount1 := ChannelAccount{PublicKey: kp1.Address(), EncryptedPrivateKey: kp1.Seed()}
	channelAccount2 := ChannelAccount{PublicKey: kp2.Address(), EncryptedPrivateKey: kp2.Seed()}
	createChannelAccountFixture(t, ctx, dbConnectionPool, channelAccount1, channelAccount2)

	t.Run("returns_error_when_there's_no_channel_account_available", func(t *testing.T) {
		const lockChannelAccountQuery = `
			UPDATE
				channel_accounts
			SET
				locked_at = NOW(),
				locked_until = NOW() + '5 minutes'::INTERVAL,
				locked_tx_hash = 'hash'
			WHERE
				public_key = ANY($1)
		`
		_, err := dbConnectionPool.Exec(ctx, lockChannelAccountQuery, []string{channelAccount1.PublicKey, channelAccount2.PublicKey})
		require.NoError(t, err)

		ca, err := m.GetAndLockIdleChannelAccount(ctx, time.Minute)
		assert.ErrorIs(t, err, ErrNoIdleChannelAccountAvailable)
		assert.Nil(t, ca)
	})

	t.Run("returns_channel_account_available", func(t *testing.T) {
		const lockChannelAccountQuery = `
				UPDATE
					channel_accounts
				SET
					locked_at = NULL,
					locked_until = NULL,
					locked_tx_hash = NULL
			`
		_, err := dbConnectionPool.Exec(ctx, lockChannelAccountQuery)
		require.NoError(t, err)

		ca, err := m.GetAndLockIdleChannelAccount(ctx, time.Minute)
		require.NoError(t, err)
		assert.Contains(t, []string{channelAccount1.PublicKey, channelAccount2.PublicKey}, ca.PublicKey)
	})

	t.Run("returns_channel_account_available_despite_locked_tx_hash", func(t *testing.T) {
		const unlockAllChannelAccountsQuery = `
				UPDATE
					channel_accounts
				SET
					locked_at = NULL,
					locked_until = NULL,
					locked_tx_hash = 'hash'
				WHERE
					public_key = $1
			`
		_, err := dbConnectionPool.Exec(ctx, unlockAllChannelAccountsQuery, channelAccount1.PublicKey)
		require.NoError(t, err)

		ca, err := m.GetAndLockIdleChannelAccount(ctx, time.Minute)
		require.NoError(t, err)
		assert.Contains(t, []string{channelAccount1.PublicKey, channelAccount2.PublicKey}, ca.PublicKey)
	})
}

func TestChannelAccountModelGet(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := NewChannelAccountModel(dbConnectionPool)

	channelAccount := keypair.MustRandom()
	ca, err := m.Get(ctx, channelAccount.Address())
	assert.ErrorIs(t, err, ErrChannelAccountNotFound)
	assert.Nil(t, ca)

	createChannelAccountFixture(t, ctx, dbConnectionPool, ChannelAccount{PublicKey: channelAccount.Address(), EncryptedPrivateKey: channelAccount.Seed()})
	ca, err = m.Get(ctx, channelAccount.Address())
	require.NoError(t, err)
	assert.Equal(t, ca.PublicKey, channelAccount.Address())
	assert.Equal(t, ca.EncryptedPrivateKey, channelAccount.Seed())
}

func TestChannelAccountModelGetAllByPublicKey(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := NewChannelAccountModel(dbConnectionPool)

	channelAccount1 := keypair.MustRandom()
	channelAccount2 := keypair.MustRandom()
	createChannelAccountFixture(t, ctx, dbConnectionPool, ChannelAccount{PublicKey: channelAccount1.Address(), EncryptedPrivateKey: channelAccount1.Seed()}, ChannelAccount{PublicKey: channelAccount2.Address(), EncryptedPrivateKey: channelAccount2.Seed()})

	channelAccounts, err := m.GetAllByPublicKey(ctx, channelAccount1.Address(), channelAccount2.Address())
	require.NoError(t, err)

	assert.Len(t, channelAccounts, 2)
	assert.Equal(t, channelAccount1.Address(), channelAccounts[0].PublicKey)
	assert.Equal(t, channelAccount2.Address(), channelAccounts[1].PublicKey)
}

func TestAssignTxToChannelAccount(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := NewChannelAccountModel(dbConnectionPool)

	channelAccount := keypair.MustRandom()
	createChannelAccountFixture(t, ctx, dbConnectionPool, ChannelAccount{PublicKey: channelAccount.Address(), EncryptedPrivateKey: channelAccount.Seed()})

	err = m.AssignTxToChannelAccount(ctx, channelAccount.Address(), "txhash")
	assert.NoError(t, err)
	channelAccountFromDB, err := m.Get(ctx, channelAccount.Address())
	assert.NoError(t, err)
	assert.Equal(t, "txhash", channelAccountFromDB.LockedTxHash.String)
}

func Test_ChannelAccountModel_UnassignTxAndUnlockChannelAccounts(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	ctx := context.Background()
	dbConnectionPool, outerErr := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, outerErr)
	defer dbConnectionPool.Close()

	m := NewChannelAccountModel(dbConnectionPool)

	testCases := []struct {
		name                string
		numberOfFixtures    int
		txHashes            func(fixtures []*keypair.Full) []string
		expectedErrContains string
	}{
		{
			name:             "🔴no_tx_hashes",
			numberOfFixtures: 0,
			txHashes: func(_ []*keypair.Full) []string {
				return nil
			},
			expectedErrContains: "txHashes cannot be empty",
		},
		{
			name:             "🟡tx_hashes_not_found",
			numberOfFixtures: 0,
			txHashes: func(_ []*keypair.Full) []string {
				return []string{"not_found_1", "not_found_2"}
			},
		},
		{
			name:             "🟢single_tx_hash_found",
			numberOfFixtures: 1,
			txHashes: func(fixtures []*keypair.Full) []string {
				return []string{"txhash_" + fixtures[0].Address()}
			},
		},
		{
			name:             "🟢multiple_tx_hashes_all_found",
			numberOfFixtures: 2,
			txHashes: func(fixtures []*keypair.Full) []string {
				return []string{"txhash_" + fixtures[0].Address(), "txhash_" + fixtures[1].Address()}
			},
		},
		{
			name:             "🟢multiple_tx_hashes_some_found",
			numberOfFixtures: 2,
			txHashes: func(fixtures []*keypair.Full) []string {
				return []string{"txhash_" + fixtures[0].Address(), "txhash_" + fixtures[1].Address(), "txhash_" + fixtures[0].Address(), "not_found_1"}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				_, err := dbConnectionPool.Exec(ctx, `DELETE from channel_accounts`)
				require.NoError(t, err)
			}()

			// Create fixtures for this test case
			fixtures := make([]*keypair.Full, tc.numberOfFixtures)
			now := time.Now()
			for i := range fixtures {
				channelAccount := keypair.MustRandom()
				createChannelAccountFixture(t, ctx, dbConnectionPool, ChannelAccount{
					PublicKey:           channelAccount.Address(),
					EncryptedPrivateKey: channelAccount.Seed(),
					LockedTxHash:        utils.SQLNullString("txhash_" + channelAccount.Address()),
					LockedAt:            utils.SQLNullTime(now),
					LockedUntil:         utils.SQLNullTime(now.Add(time.Minute)),
				})
				fixtures[i] = channelAccount
			}

			// 🔒 Channel accounts start locked
			for _, fixture := range fixtures {
				chAccFromDB, err := m.Get(ctx, fixture.Address())
				require.NoError(t, err)
				require.True(t, chAccFromDB.LockedTxHash.Valid)
				require.True(t, chAccFromDB.LockedAt.Valid)
				require.True(t, chAccFromDB.LockedUntil.Valid)
			}

			// Start pgx transaction for the unlock call
			pgxTx, err := dbConnectionPool.Begin(ctx)
			require.NoError(t, err)
			defer pgxTx.Rollback(ctx) //nolint:errcheck

			rowsAffected, err := m.UnassignTxAndUnlockChannelAccounts(ctx, pgxTx, tc.txHashes(fixtures)...)
			if tc.expectedErrContains != "" {
				require.ErrorContains(t, err, tc.expectedErrContains)
			} else {
				require.NoError(t, err)
				require.Equal(t, int64(tc.numberOfFixtures), rowsAffected)
				// Commit the pgx transaction to persist the changes
				require.NoError(t, pgxTx.Commit(ctx))
				// 🔓 Channel accounts get unlocked
				for _, fixture := range fixtures {
					chAccFromDB, err := m.Get(ctx, fixture.Address())
					require.NoError(t, err)
					require.False(t, chAccFromDB.LockedTxHash.Valid)
					require.False(t, chAccFromDB.LockedAt.Valid)
					require.False(t, chAccFromDB.LockedUntil.Valid)
				}
			}
		})
	}
}

func TestChannelAccountModelBatchInsert(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := NewChannelAccountModel(dbConnectionPool)

	t.Run("channel_accounts_empty", func(t *testing.T) {
		err = m.BatchInsert(ctx, []*ChannelAccount{})
		require.NoError(t, err)
	})

	t.Run("invalid_channel_accounts", func(t *testing.T) {
		channelAccounts := []*ChannelAccount{
			{
				PublicKey: "",
			},
		}
		err = m.BatchInsert(ctx, channelAccounts)
		assert.EqualError(t, err, "public key cannot be empty")

		channelAccounts = []*ChannelAccount{
			{
				PublicKey: keypair.MustRandom().Address(),
			},
		}
		err = m.BatchInsert(ctx, channelAccounts)
		assert.EqualError(t, err, "private key cannot be empty")
	})

	t.Run("inserts_channel_accounts_successfully", func(t *testing.T) {
		channelAccount1 := keypair.MustRandom()
		channelAccount2 := keypair.MustRandom()
		channelAccounts := []*ChannelAccount{
			{
				PublicKey:           channelAccount1.Address(),
				EncryptedPrivateKey: channelAccount1.Seed(),
			},
			{
				PublicKey:           channelAccount2.Address(),
				EncryptedPrivateKey: channelAccount2.Seed(),
			},
		}
		err = m.BatchInsert(ctx, channelAccounts)
		require.NoError(t, err)

		n, err := m.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(2), n)
	})
}

func TestChannelAccountModelCount(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := NewChannelAccountModel(dbConnectionPool)

	n, err := m.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	channelAccount := keypair.MustRandom()
	createChannelAccountFixture(t, ctx, dbConnectionPool, ChannelAccount{PublicKey: channelAccount.Address(), EncryptedPrivateKey: channelAccount.Seed()})

	n, err = m.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestChannelAccountModelGetAll(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := NewChannelAccountModel(dbConnectionPool)

	testCases := []struct {
		name             string
		numberOfFixtures int
		limit            int
		expectedCount    int
	}{
		{
			name:             "🟡empty_database",
			numberOfFixtures: 0,
			limit:            10,
			expectedCount:    0,
		},
		{
			name:             "🟢limit_greater_than_fixtures",
			numberOfFixtures: 2,
			limit:            10,
			expectedCount:    2,
		},
		{
			name:             "🟢limit_less_than_fixtures",
			numberOfFixtures: 5,
			limit:            3,
			expectedCount:    3,
		},
		{
			name:             "🟢exact_limit_match",
			numberOfFixtures: 3,
			limit:            3,
			expectedCount:    3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				_, err := dbConnectionPool.Exec(ctx, `DELETE from channel_accounts`)
				require.NoError(t, err)
			}()

			// Create fixtures
			for i := 0; i < tc.numberOfFixtures; i++ {
				channelAccount := keypair.MustRandom()
				createChannelAccountFixture(t, ctx, dbConnectionPool, ChannelAccount{
					PublicKey:           channelAccount.Address(),
					EncryptedPrivateKey: channelAccount.Seed(),
				})
			}

			pgxTx, err := dbConnectionPool.Begin(ctx)
			require.NoError(t, err)
			defer pgxTx.Rollback(ctx) //nolint:errcheck
			accounts, err := m.GetAll(ctx, pgxTx, tc.limit)
			require.NoError(t, err)
			require.NoError(t, pgxTx.Commit(ctx))
			assert.Len(t, accounts, tc.expectedCount)
		})
	}
}

func TestChannelAccountModelDelete(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := NewChannelAccountModel(dbConnectionPool)

	testCases := []struct {
		name                 string
		accountsToCreate     int
		wantAccountsToDelete int
		publicKeys           []string
		expectedErrIs        error
	}{
		{
			name:                 "🟡account_not_found",
			accountsToCreate:     0,
			wantAccountsToDelete: 0,
			publicKeys:           []string{"GINVALID123456789"},
		},
		{
			name:                 "🟢create_1_delete_1",
			accountsToCreate:     1,
			wantAccountsToDelete: 1,
			publicKeys:           nil, // Will be set in test
		},
		{
			name:                 "🟢create_2_delete_2",
			accountsToCreate:     2,
			wantAccountsToDelete: 2,
			publicKeys:           nil, // Will be set in test
		},
		{
			name:                 "🟢create_2_delete_1",
			accountsToCreate:     2,
			wantAccountsToDelete: 1,
			publicKeys:           nil, // Will be set in test
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				_, err := dbConnectionPool.Exec(ctx, `DELETE from channel_accounts`)
				require.NoError(t, err)
			}()

			publicKeys := tc.publicKeys
			accountsToCreate := tc.accountsToCreate
			for accountsToCreate > 0 {
				channelAccount := keypair.MustRandom()
				publicKeys = append(publicKeys, channelAccount.Address())
				createChannelAccountFixture(t, ctx, dbConnectionPool, ChannelAccount{
					PublicKey:           channelAccount.Address(),
					EncryptedPrivateKey: channelAccount.Seed(),
				})
				accountsToCreate--
			}

			pgxTx, err := dbConnectionPool.Begin(ctx)
			require.NoError(t, err)
			defer pgxTx.Rollback(ctx) //nolint:errcheck
			rowsAffected, err := m.Delete(ctx, pgxTx, publicKeys[:tc.wantAccountsToDelete]...)
			require.NoError(t, err)
			require.NoError(t, pgxTx.Commit(ctx))
			require.Equal(t, tc.wantAccountsToDelete, int(rowsAffected))

			if tc.wantAccountsToDelete > 0 {
				// Verify account was deleted
				n, err := m.Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, tc.accountsToCreate-tc.wantAccountsToDelete, int(n))
			}
		})
	}
}
