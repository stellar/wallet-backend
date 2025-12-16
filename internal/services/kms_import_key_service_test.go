package services

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/signing/awskms"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/utils"
)

func TestKMSImportServiceImportDistributionAccountKey(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	kmsClient := awskms.KMSMock{}
	keypairStore := store.KeypairStoreMock{}
	distributionAccount := keypair.MustRandom()
	s, err := NewKMSImportService(&kmsClient, "aws-arn", &keypairStore, distributionAccount.Address())
	require.NoError(t, err)

	t.Run("invalid_private_key", func(t *testing.T) {
		err := s.ImportDistributionAccountKey(ctx, "invalid")
		assert.ErrorIs(t, err, ErrInvalidPrivateKeyProvided)
	})

	t.Run("mismatch_distribution_account", func(t *testing.T) {
		err := s.ImportDistributionAccountKey(ctx, keypair.MustRandom().Seed())
		assert.ErrorIs(t, err, ErrMismatchDistributionAccount)
	})

	t.Run("returns_error_when_fails_to_encrypt", func(t *testing.T) {
		kmsClient.
			On("Encrypt", &kms.EncryptInput{
				EncryptionContext: map[string]*string{
					"pubkey": utils.PointOf(distributionAccount.Address()),
				},
				KeyId:     utils.PointOf("aws-arn"),
				Plaintext: []byte(distributionAccount.Seed()),
			}).
			Return(nil, errors.New("unexpected error")).
			Once()

		err := s.ImportDistributionAccountKey(ctx, distributionAccount.Seed())
		assert.EqualError(t, err, "encrypting distribution account private key: unexpected error")
	})

	t.Run("returns_error_when_fails_to_store_the_encrypted_key", func(t *testing.T) {
		kmsClient.
			On("Encrypt", &kms.EncryptInput{
				EncryptionContext: map[string]*string{
					"pubkey": utils.PointOf(distributionAccount.Address()),
				},
				KeyId:     utils.PointOf("aws-arn"),
				Plaintext: []byte(distributionAccount.Seed()),
			}).
			Return(&kms.EncryptOutput{
				CiphertextBlob: []byte("encrypted"),
			}, nil).
			Once()

		keypairStore.
			On("Insert", ctx, distributionAccount.Address(), []byte("encrypted")).
			Return(store.ErrKeypairNotFound).
			Once()

		err := s.ImportDistributionAccountKey(ctx, distributionAccount.Seed())
		assert.ErrorIs(t, err, store.ErrKeypairNotFound)
	})

	t.Run("successfully_import_distribution_account_private_key", func(t *testing.T) {
		kmsClient.
			On("Encrypt", &kms.EncryptInput{
				EncryptionContext: map[string]*string{
					"pubkey": utils.PointOf(distributionAccount.Address()),
				},
				KeyId:     utils.PointOf("aws-arn"),
				Plaintext: []byte(distributionAccount.Seed()),
			}).
			Return(&kms.EncryptOutput{
				CiphertextBlob: []byte("encrypted"),
			}, nil).
			Once()

		keypairStore.
			On("Insert", ctx, distributionAccount.Address(), []byte("encrypted")).
			Return(nil).
			Once()

		err := s.ImportDistributionAccountKey(ctx, distributionAccount.Seed())
		assert.NoError(t, err)
	})
}
