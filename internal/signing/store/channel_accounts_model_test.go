package store

import (
	"context"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/utils"
)

func createChannelAccountFixture(t *testing.T, ctx context.Context, dbConnectionPool db.ConnectionPool, channelAccounts ...ChannelAccount) {
	t.Helper()
	if len(channelAccounts) == 0 {
		return
	}
	const q = `
		INSERT INTO
			channel_accounts (public_key, encrypted_private_key, locked_tx_hash, locked_at, locked_until)
		VALUES
			(:public_key, :encrypted_private_key, :locked_tx_hash, :locked_at, :locked_until)
	`
	_, err := dbConnectionPool.NamedExecContext(ctx, q, channelAccounts)
	require.NoError(t, err)
}

func TestChannelAccountModelGetAndLockIdleChannelAccount(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
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
		_, err := dbConnectionPool.ExecContext(ctx, lockChannelAccountQuery, pq.Array([]string{channelAccount1.PublicKey, channelAccount2.PublicKey}))
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
		_, err := dbConnectionPool.ExecContext(ctx, lockChannelAccountQuery)
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
		_, err := dbConnectionPool.ExecContext(ctx, unlockAllChannelAccountsQuery, channelAccount1.PublicKey)
		require.NoError(t, err)

		ca, err := m.GetAndLockIdleChannelAccount(ctx, time.Minute)
		require.NoError(t, err)
		assert.Contains(t, []string{channelAccount1.PublicKey, channelAccount2.PublicKey}, ca.PublicKey)
	})
}

func TestChannelAccountModelGet(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	m := NewChannelAccountModel(dbConnectionPool)

	channelAccount := keypair.MustRandom()
	ca, err := m.Get(ctx, dbConnectionPool, channelAccount.Address())
	assert.ErrorIs(t, err, ErrChannelAccountNotFound)
	assert.Nil(t, ca)

	createChannelAccountFixture(t, ctx, dbConnectionPool, ChannelAccount{PublicKey: channelAccount.Address(), EncryptedPrivateKey: channelAccount.Seed()})
	ca, err = m.Get(ctx, dbConnectionPool, channelAccount.Address())
	require.NoError(t, err)
	assert.Equal(t, ca.PublicKey, channelAccount.Address())
	assert.Equal(t, ca.EncryptedPrivateKey, channelAccount.Seed())
}

func TestChannelAccountModelGetAllByPublicKey(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	m := NewChannelAccountModel(dbConnectionPool)

	channelAccount1 := keypair.MustRandom()
	channelAccount2 := keypair.MustRandom()
	createChannelAccountFixture(t, ctx, dbConnectionPool, ChannelAccount{PublicKey: channelAccount1.Address(), EncryptedPrivateKey: channelAccount1.Seed()}, ChannelAccount{PublicKey: channelAccount2.Address(), EncryptedPrivateKey: channelAccount2.Seed()})

	channelAccounts, err := m.GetAllByPublicKey(ctx, dbConnectionPool, channelAccount1.Address(), channelAccount2.Address())
	require.NoError(t, err)

	assert.Len(t, channelAccounts, 2)
	assert.Equal(t, channelAccount1.Address(), channelAccounts[0].PublicKey)
	assert.Equal(t, channelAccount2.Address(), channelAccounts[1].PublicKey)
}

func TestAssignTxToChannelAccount(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	m := NewChannelAccountModel(dbConnectionPool)

	channelAccount := keypair.MustRandom()
	createChannelAccountFixture(t, ctx, dbConnectionPool, ChannelAccount{PublicKey: channelAccount.Address(), EncryptedPrivateKey: channelAccount.Seed()})

	err = m.AssignTxToChannelAccount(ctx, channelAccount.Address(), "txhash")
	assert.NoError(t, err)
	channelAccountFromDB, err := m.Get(ctx, dbConnectionPool, channelAccount.Address())
	assert.NoError(t, err)
	assert.Equal(t, "txhash", channelAccountFromDB.LockedTxHash.String)
}

func Test_ChannelAccountModel_UnassignTxAndUnlockChannelAccounts(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, outerErr := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, outerErr)
	defer dbConnectionPool.Close()

	ctx := context.Background()
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
				_, err := dbConnectionPool.ExecContext(ctx, `DELETE from channel_accounts`)
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
				chAccFromDB, err := m.Get(ctx, dbConnectionPool, fixture.Address())
				require.NoError(t, err)
				require.True(t, chAccFromDB.LockedTxHash.Valid)
				require.True(t, chAccFromDB.LockedAt.Valid)
				require.True(t, chAccFromDB.LockedUntil.Valid)
			}

			rowsAffected, err := m.UnassignTxAndUnlockChannelAccounts(ctx, tc.txHashes(fixtures)...)
			if tc.expectedErrContains != "" {
				require.ErrorContains(t, err, tc.expectedErrContains)
			} else {
				require.NoError(t, err)
				require.Equal(t, int64(tc.numberOfFixtures), rowsAffected)
				// 🔓 Channel accounts get unlocked
				for _, fixture := range fixtures {
					chAccFromDB, err := m.Get(ctx, dbConnectionPool, fixture.Address())
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
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	m := NewChannelAccountModel(dbConnectionPool)

	t.Run("channel_accounts_empty", func(t *testing.T) {
		err = m.BatchInsert(ctx, dbConnectionPool, []*ChannelAccount{})
		require.NoError(t, err)
	})

	t.Run("invalid_channel_accounts", func(t *testing.T) {
		channelAccounts := []*ChannelAccount{
			{
				PublicKey: "",
			},
		}
		err = m.BatchInsert(ctx, dbConnectionPool, channelAccounts)
		assert.EqualError(t, err, "public key cannot be empty")

		channelAccounts = []*ChannelAccount{
			{
				PublicKey: keypair.MustRandom().Address(),
			},
		}
		err = m.BatchInsert(ctx, dbConnectionPool, channelAccounts)
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
		err = m.BatchInsert(ctx, dbConnectionPool, channelAccounts)
		require.NoError(t, err)

		n, err := m.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(2), n)
	})
}

func TestChannelAccountModelCount(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
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
