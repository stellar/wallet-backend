package channelaccounts

import (
	"context"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createChannelAccountFixture(t *testing.T, ctx context.Context, dbConnectionPool db.ConnectionPool, publicKey, encryptedPrivateKey string) {
	t.Helper()
	const q = `
		INSERT INTO 
			channel_accounts (public_key, encrypted_private_key)
		VALUES
			($1, $2)
	`
	_, err := dbConnectionPool.ExecContext(ctx, q, publicKey, encryptedPrivateKey)
	require.NoError(t, err)
}

func TestChannelAccountModelGetIdleChannelAccount(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	m := NewChannelAccountModel(dbConnectionPool)

	t.Run("returns_error_when_there's_no_channel_account_available", func(t *testing.T) {
		channelAccount1 := keypair.MustRandom()
		channelAccount2 := keypair.MustRandom()
		createChannelAccountFixture(t, ctx, dbConnectionPool, channelAccount1.Address(), channelAccount1.Seed())
		createChannelAccountFixture(t, ctx, dbConnectionPool, channelAccount2.Address(), channelAccount2.Seed())

		const lockChannelAccountQuery = `
			UPDATE
				channel_accounts
			SET
				locked_at = NOW(),
				locked_until = NOW() + '5 minutes'::INTERVAL
			WHERE
				public_key = $1
		`
		_, err := dbConnectionPool.ExecContext(ctx, lockChannelAccountQuery, channelAccount1.Address())
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, lockChannelAccountQuery, channelAccount2.Address())
		require.NoError(t, err)

		ca, err := m.GetIdleChannelAccount(ctx, time.Minute)
		assert.ErrorIs(t, err, ErrNoIdleChannelAccountAvailable)
		assert.Nil(t, ca)
	})

	t.Run("returns_error_when_there's_no_channel_account_available", func(t *testing.T) {
		channelAccount1 := keypair.MustRandom()
		channelAccount2 := keypair.MustRandom()
		createChannelAccountFixture(t, ctx, dbConnectionPool, channelAccount1.Address(), channelAccount1.Seed())
		createChannelAccountFixture(t, ctx, dbConnectionPool, channelAccount2.Address(), channelAccount2.Seed())

		const lockChannelAccountQuery = `
			UPDATE
				channel_accounts
			SET
				locked_at = NOW(),
				locked_until = NOW() + '5 minutes'::INTERVAL
			WHERE
				public_key = $1
		`
		_, err := dbConnectionPool.ExecContext(ctx, lockChannelAccountQuery, channelAccount1.Address())
		require.NoError(t, err)

		ca, err := m.GetIdleChannelAccount(ctx, time.Minute)
		require.NoError(t, err)
		assert.Equal(t, ca.PublicKey, channelAccount2.Address())
		assert.Equal(t, ca.EncryptedPrivateKey, channelAccount2.Seed())
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

	createChannelAccountFixture(t, ctx, dbConnectionPool, channelAccount.Address(), channelAccount.Seed())
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
	createChannelAccountFixture(t, ctx, dbConnectionPool, channelAccount1.Address(), channelAccount1.Seed())
	createChannelAccountFixture(t, ctx, dbConnectionPool, channelAccount2.Address(), channelAccount2.Seed())

	channelAccounts, err := m.GetAllByPublicKey(ctx, dbConnectionPool, channelAccount1.Address(), channelAccount2.Address())
	require.NoError(t, err)

	assert.Len(t, channelAccounts, 2)
	assert.Equal(t, channelAccount1.Address(), channelAccounts[0].PublicKey)
	assert.Equal(t, channelAccount2.Address(), channelAccounts[1].PublicKey)
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
		err := m.BatchInsert(ctx, dbConnectionPool, []*ChannelAccount{})
		require.NoError(t, err)
	})

	t.Run("invalid_channel_accounts", func(t *testing.T) {
		channelAccounts := []*ChannelAccount{
			{
				PublicKey: "",
			},
		}
		err := m.BatchInsert(ctx, dbConnectionPool, channelAccounts)
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
	createChannelAccountFixture(t, ctx, dbConnectionPool, channelAccount.Address(), channelAccount.Seed())
	n, err = m.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}
