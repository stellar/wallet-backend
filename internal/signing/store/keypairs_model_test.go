package store

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

func createKeypairFixture(t *testing.T, ctx context.Context, dbConnectionPool db.ConnectionPool, kp Keypair) {
	t.Helper()
	const q = `
		INSERT INTO
			keypairs (public_key, encrypted_private_key)
		VALUES
			($1, $2)
	`
	_, err := dbConnectionPool.Pool().Exec(ctx, q, kp.PublicKey, kp.EncryptedPrivateKey)
	require.NoError(t, err)
}

func TestKeypairModelGetByPublicKey(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	m := NewKeypairModel(dbConnectionPool)

	t.Run("keypair_not_found", func(t *testing.T) {
		kp, err := m.GetByPublicKey(ctx, "unknown")
		assert.ErrorIs(t, err, ErrKeypairNotFound)
		assert.Nil(t, kp)
	})

	t.Run("successfully_gets_the_keypair", func(t *testing.T) {
		kpFull := keypair.MustRandom()
		createKeypairFixture(t, ctx, dbConnectionPool, Keypair{PublicKey: kpFull.Address(), EncryptedPrivateKey: []byte(kpFull.Seed())})
		kp, err := m.GetByPublicKey(ctx, kpFull.Address())
		require.NoError(t, err)
		assert.Equal(t, kpFull.Address(), kp.PublicKey)
		assert.Equal(t, kpFull.Seed(), string(kp.EncryptedPrivateKey))
	})
}

func TestKeypairModelInsert(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	m := NewKeypairModel(dbConnectionPool)

	t.Run("keypair_already_exists", func(t *testing.T) {
		kpFull := keypair.MustRandom()
		createKeypairFixture(t, ctx, dbConnectionPool, Keypair{PublicKey: kpFull.Address(), EncryptedPrivateKey: []byte(kpFull.Seed())})
		err = m.Insert(ctx, kpFull.Address(), []byte(kpFull.Seed()))
		assert.ErrorIs(t, err, ErrPublicKeyAlreadyExists)
	})

	t.Run("inserts_keypair_successfully", func(t *testing.T) {
		kpFull := keypair.MustRandom()
		err = m.Insert(ctx, kpFull.Address(), []byte(kpFull.Seed()))
		require.NoError(t, err)
	})
}
