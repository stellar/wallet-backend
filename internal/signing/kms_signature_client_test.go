package signing

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/signing/awskms"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/utils"
)

func TestKMSSignatureClientGetAccountPublicKey(t *testing.T) {
	ctx := context.Background()
	distributionAccount := keypair.MustRandom()
	sc := kmsSignatureClient{distributionAccountPublicKey: distributionAccount.Address()}
	publicKey, err := sc.GetAccountPublicKey(ctx)
	require.NoError(t, err)
	assert.Equal(t, distributionAccount.Address(), publicKey)
}

func TestKMSSignatureClientNetworkPassphrase(t *testing.T) {
	sc := kmsSignatureClient{networkPassphrase: network.TestNetworkPassphrase}
	assert.Equal(t, network.TestNetworkPassphrase, sc.NetworkPassphrase())
}

func TestKMSSignatureClientSignStellarTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	distributionAccount := keypair.MustRandom()
	kmsClient := awskms.KMSMock{}
	keypairStore := store.KeypairStoreMock{}
	sc, err := NewKMSSignatureClient(distributionAccount.Address(), network.TestNetworkPassphrase, &keypairStore, &kmsClient, "aws-arn")
	require.NoError(t, err)

	t.Run("invalid_tx", func(t *testing.T) {
		signedTx, err := sc.SignStellarTransaction(ctx, nil)
		assert.ErrorIs(t, ErrInvalidTransaction, err)
		assert.Nil(t, signedTx)
	})

	t.Run("no_stellar_accounts", func(t *testing.T) {
		sourceAccount := txnbuild.NewSimpleAccount(distributionAccount.Address(), int64(9605939170639897))
		tx, err := txnbuild.NewTransaction(
			txnbuild.TransactionParams{
				SourceAccount:        &sourceAccount,
				IncrementSequenceNum: true,
				Operations: []txnbuild.Operation{
					&txnbuild.Payment{
						Destination: "GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z",
						Amount:      "10",
						Asset:       txnbuild.NativeAsset{},
					},
				},
				BaseFee:       txnbuild.MinBaseFee,
				Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
			},
		)
		require.NoError(t, err)

		signedTx, err := sc.SignStellarTransaction(ctx, tx)
		assert.EqualError(t, err, "stellar accounts cannot be empty in *signing.kmsSignatureClient")
		assert.Nil(t, signedTx)
	})

	t.Run("return_errors_when_the_accounts_are_not_the_distribution_account", func(t *testing.T) {
		sourceAccount := txnbuild.NewSimpleAccount(distributionAccount.Address(), int64(9605939170639897))
		tx, err := txnbuild.NewTransaction(
			txnbuild.TransactionParams{
				SourceAccount:        &sourceAccount,
				IncrementSequenceNum: true,
				Operations: []txnbuild.Operation{
					&txnbuild.Payment{
						Destination: "GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z",
						Amount:      "10",
						Asset:       txnbuild.NativeAsset{},
					},
				},
				BaseFee:       txnbuild.MinBaseFee,
				Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
			},
		)
		require.NoError(t, err)

		acc := keypair.MustRandom()
		signedTx, err := sc.SignStellarTransaction(ctx, tx, acc.Address())
		assert.EqualError(t, err, fmt.Sprintf("stellar account %s is not allowed to sign *signing.kmsSignatureClient", acc.Address()))
		assert.Nil(t, signedTx)
	})

	t.Run("keypair_not_found", func(t *testing.T) {
		sourceAccount := txnbuild.NewSimpleAccount(distributionAccount.Address(), int64(9605939170639897))
		tx, err := txnbuild.NewTransaction(
			txnbuild.TransactionParams{
				SourceAccount:        &sourceAccount,
				IncrementSequenceNum: true,
				Operations: []txnbuild.Operation{
					&txnbuild.Payment{
						Destination: "GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z",
						Amount:      "10",
						Asset:       txnbuild.NativeAsset{},
					},
				},
				BaseFee:       txnbuild.MinBaseFee,
				Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
			},
		)
		require.NoError(t, err)

		keypairStore.
			On("GetByPublicKey", ctx, distributionAccount.Address()).
			Return(nil, store.ErrKeypairNotFound).
			Once()
		defer keypairStore.AssertExpectations(t)

		signedTx, err := sc.SignStellarTransaction(ctx, tx, distributionAccount.Address())
		assert.ErrorIs(t, err, store.ErrKeypairNotFound)
		assert.Nil(t, signedTx)
	})

	t.Run("kms_decrypting_fails", func(t *testing.T) {
		sourceAccount := txnbuild.NewSimpleAccount(distributionAccount.Address(), int64(9605939170639897))
		tx, err := txnbuild.NewTransaction(
			txnbuild.TransactionParams{
				SourceAccount:        &sourceAccount,
				IncrementSequenceNum: true,
				Operations: []txnbuild.Operation{
					&txnbuild.Payment{
						Destination: "GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z",
						Amount:      "10",
						Asset:       txnbuild.NativeAsset{},
					},
				},
				BaseFee:       txnbuild.MinBaseFee,
				Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
			},
		)
		require.NoError(t, err)

		keypairStore.
			On("GetByPublicKey", ctx, distributionAccount.Address()).
			Return(&store.Keypair{
				PublicKey:           distributionAccount.Address(),
				EncryptedPrivateKey: []byte("encrypted"),
			}, nil).
			Once()
		defer keypairStore.AssertExpectations(t)

		kmsClient.
			On("Decrypt", &kms.DecryptInput{
				CiphertextBlob:    []byte("encrypted"),
				EncryptionContext: awskms.GetPrivateKeyEncryptionContext(distributionAccount.Address()),
				KeyId:             utils.PointOf("aws-arn"),
			}).
			Return(nil, errors.New("unexpected error")).
			Once()
		defer kmsClient.AssertExpectations(t)

		signedTx, err := sc.SignStellarTransaction(ctx, tx, distributionAccount.Address())
		assert.EqualError(t, err, "decrypting distribution account private key in *signing.kmsSignatureClient: unexpected error")
		assert.Nil(t, signedTx)
	})

	t.Run("kms_decrypt_an_invalid_secret_key", func(t *testing.T) {
		sourceAccount := txnbuild.NewSimpleAccount(distributionAccount.Address(), int64(9605939170639897))
		tx, err := txnbuild.NewTransaction(
			txnbuild.TransactionParams{
				SourceAccount:        &sourceAccount,
				IncrementSequenceNum: true,
				Operations: []txnbuild.Operation{
					&txnbuild.Payment{
						Destination: "GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z",
						Amount:      "10",
						Asset:       txnbuild.NativeAsset{},
					},
				},
				BaseFee:       txnbuild.MinBaseFee,
				Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
			},
		)
		require.NoError(t, err)

		keypairStore.
			On("GetByPublicKey", ctx, distributionAccount.Address()).
			Return(&store.Keypair{
				PublicKey:           distributionAccount.Address(),
				EncryptedPrivateKey: []byte("encrypted"),
			}, nil).
			Once()
		defer keypairStore.AssertExpectations(t)

		kmsClient.
			On("Decrypt", &kms.DecryptInput{
				CiphertextBlob:    []byte("encrypted"),
				EncryptionContext: awskms.GetPrivateKeyEncryptionContext(distributionAccount.Address()),
				KeyId:             utils.PointOf("aws-arn"),
			}).
			Return(&kms.DecryptOutput{Plaintext: []byte("invalid")}, nil).
			Once()
		defer kmsClient.AssertExpectations(t)

		signedTx, err := sc.SignStellarTransaction(ctx, tx, distributionAccount.Address())
		assert.EqualError(t, err, "parsing distribution account private key in *signing.kmsSignatureClient: base32 decode failed: illegal base32 data at input byte 7")
		assert.Nil(t, signedTx)
	})

	t.Run("successfully_signs_the_transaction", func(t *testing.T) {
		sourceAccount := txnbuild.NewSimpleAccount(distributionAccount.Address(), int64(9605939170639897))
		tx, err := txnbuild.NewTransaction(
			txnbuild.TransactionParams{
				SourceAccount:        &sourceAccount,
				IncrementSequenceNum: true,
				Operations: []txnbuild.Operation{
					&txnbuild.Payment{
						Destination: "GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z",
						Amount:      "10",
						Asset:       txnbuild.NativeAsset{},
					},
				},
				BaseFee:       txnbuild.MinBaseFee,
				Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
			},
		)
		require.NoError(t, err)

		expectedSignedTx, err := tx.Sign(network.TestNetworkPassphrase, distributionAccount)
		require.NoError(t, err)

		keypairStore.
			On("GetByPublicKey", ctx, distributionAccount.Address()).
			Return(&store.Keypair{
				PublicKey:           distributionAccount.Address(),
				EncryptedPrivateKey: []byte("encrypted"),
			}, nil).
			Once()
		defer keypairStore.AssertExpectations(t)

		kmsClient.
			On("Decrypt", &kms.DecryptInput{
				CiphertextBlob:    []byte("encrypted"),
				EncryptionContext: awskms.GetPrivateKeyEncryptionContext(distributionAccount.Address()),
				KeyId:             utils.PointOf("aws-arn"),
			}).
			Return(&kms.DecryptOutput{Plaintext: []byte(distributionAccount.Seed())}, nil).
			Once()
		defer kmsClient.AssertExpectations(t)

		signedTx, err := sc.SignStellarTransaction(ctx, tx, distributionAccount.Address())
		require.NoError(t, err)
		assert.Equal(t, expectedSignedTx.Signatures(), signedTx.Signatures())
	})
}

func TestKMSSignatureClientSignStellarFeeBumpTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	distributionAccount := keypair.MustRandom()
	kmsClient := awskms.KMSMock{}
	keypairStore := store.KeypairStoreMock{}
	sc, err := NewKMSSignatureClient(distributionAccount.Address(), network.TestNetworkPassphrase, &keypairStore, &kmsClient, "aws-arn")
	require.NoError(t, err)

	t.Run("invalid_fee_bump_tx", func(t *testing.T) {
		signedFeeBumpTx, err := sc.SignStellarFeeBumpTransaction(ctx, nil)
		assert.ErrorIs(t, ErrInvalidTransaction, err)
		assert.Nil(t, signedFeeBumpTx)
	})

	t.Run("keypair_not_found", func(t *testing.T) {
		sourceAccount := txnbuild.NewSimpleAccount(distributionAccount.Address(), int64(9605939170639897))
		tx, err := txnbuild.NewTransaction(
			txnbuild.TransactionParams{
				SourceAccount:        &sourceAccount,
				IncrementSequenceNum: true,
				Operations: []txnbuild.Operation{
					&txnbuild.Payment{
						Destination: "GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z",
						Amount:      "10",
						Asset:       txnbuild.NativeAsset{},
					},
				},
				BaseFee:       txnbuild.MinBaseFee,
				Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
			},
		)
		require.NoError(t, err)

		feeBumpTx, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
			Inner:      tx,
			FeeAccount: distributionAccount.Address(),
			BaseFee:    txnbuild.MinBaseFee,
		})
		require.NoError(t, err)

		keypairStore.
			On("GetByPublicKey", ctx, distributionAccount.Address()).
			Return(nil, store.ErrKeypairNotFound).
			Once()
		defer keypairStore.AssertExpectations(t)

		signedFeeBumpTx, err := sc.SignStellarFeeBumpTransaction(ctx, feeBumpTx)
		assert.ErrorIs(t, err, store.ErrKeypairNotFound)
		assert.Nil(t, signedFeeBumpTx)
	})

	t.Run("kms_decrypting_fails", func(t *testing.T) {
		sourceAccount := txnbuild.NewSimpleAccount(distributionAccount.Address(), int64(9605939170639897))
		tx, err := txnbuild.NewTransaction(
			txnbuild.TransactionParams{
				SourceAccount:        &sourceAccount,
				IncrementSequenceNum: true,
				Operations: []txnbuild.Operation{
					&txnbuild.Payment{
						Destination: "GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z",
						Amount:      "10",
						Asset:       txnbuild.NativeAsset{},
					},
				},
				BaseFee:       txnbuild.MinBaseFee,
				Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
			},
		)
		require.NoError(t, err)

		feeBumpTx, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
			Inner:      tx,
			FeeAccount: distributionAccount.Address(),
			BaseFee:    txnbuild.MinBaseFee,
		})
		require.NoError(t, err)

		keypairStore.
			On("GetByPublicKey", ctx, distributionAccount.Address()).
			Return(&store.Keypair{
				PublicKey:           distributionAccount.Address(),
				EncryptedPrivateKey: []byte("encrypted"),
			}, nil).
			Once()
		defer keypairStore.AssertExpectations(t)

		kmsClient.
			On("Decrypt", &kms.DecryptInput{
				CiphertextBlob:    []byte("encrypted"),
				EncryptionContext: awskms.GetPrivateKeyEncryptionContext(distributionAccount.Address()),
				KeyId:             utils.PointOf("aws-arn"),
			}).
			Return(nil, errors.New("unexpected error")).
			Once()
		defer kmsClient.AssertExpectations(t)

		signedFeeBumpTx, err := sc.SignStellarFeeBumpTransaction(ctx, feeBumpTx)
		assert.EqualError(t, err, "decrypting distribution account private key in *signing.kmsSignatureClient: unexpected error")
		assert.Nil(t, signedFeeBumpTx)
	})

	t.Run("kms_decrypt_an_invalid_secret_key", func(t *testing.T) {
		sourceAccount := txnbuild.NewSimpleAccount(distributionAccount.Address(), int64(9605939170639897))
		tx, err := txnbuild.NewTransaction(
			txnbuild.TransactionParams{
				SourceAccount:        &sourceAccount,
				IncrementSequenceNum: true,
				Operations: []txnbuild.Operation{
					&txnbuild.Payment{
						Destination: "GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z",
						Amount:      "10",
						Asset:       txnbuild.NativeAsset{},
					},
				},
				BaseFee:       txnbuild.MinBaseFee,
				Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
			},
		)
		require.NoError(t, err)

		feeBumpTx, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
			Inner:      tx,
			FeeAccount: distributionAccount.Address(),
			BaseFee:    txnbuild.MinBaseFee,
		})
		require.NoError(t, err)

		keypairStore.
			On("GetByPublicKey", ctx, distributionAccount.Address()).
			Return(&store.Keypair{
				PublicKey:           distributionAccount.Address(),
				EncryptedPrivateKey: []byte("encrypted"),
			}, nil).
			Once()
		defer keypairStore.AssertExpectations(t)

		kmsClient.
			On("Decrypt", &kms.DecryptInput{
				CiphertextBlob:    []byte("encrypted"),
				EncryptionContext: awskms.GetPrivateKeyEncryptionContext(distributionAccount.Address()),
				KeyId:             utils.PointOf("aws-arn"),
			}).
			Return(&kms.DecryptOutput{Plaintext: []byte("invalid")}, nil).
			Once()
		defer kmsClient.AssertExpectations(t)

		signedFeeBumpTx, err := sc.SignStellarFeeBumpTransaction(ctx, feeBumpTx)
		assert.EqualError(t, err, "parsing distribution account private key in *signing.kmsSignatureClient: base32 decode failed: illegal base32 data at input byte 7")
		assert.Nil(t, signedFeeBumpTx)
	})

	t.Run("successfully_signs_the_fee_bump_transaction", func(t *testing.T) {
		sourceAccount := txnbuild.NewSimpleAccount(distributionAccount.Address(), int64(9605939170639897))
		tx, err := txnbuild.NewTransaction(
			txnbuild.TransactionParams{
				SourceAccount:        &sourceAccount,
				IncrementSequenceNum: true,
				Operations: []txnbuild.Operation{
					&txnbuild.Payment{
						Destination: "GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z",
						Amount:      "10",
						Asset:       txnbuild.NativeAsset{},
					},
				},
				BaseFee:       txnbuild.MinBaseFee,
				Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
			},
		)
		require.NoError(t, err)

		feeBumpTx, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
			Inner:      tx,
			FeeAccount: distributionAccount.Address(),
			BaseFee:    txnbuild.MinBaseFee,
		})
		require.NoError(t, err)

		expectedSignedFeeBumpTx, err := feeBumpTx.Sign(network.TestNetworkPassphrase, distributionAccount)
		require.NoError(t, err)

		keypairStore.
			On("GetByPublicKey", ctx, distributionAccount.Address()).
			Return(&store.Keypair{
				PublicKey:           distributionAccount.Address(),
				EncryptedPrivateKey: []byte("encrypted"),
			}, nil).
			Once()
		defer keypairStore.AssertExpectations(t)

		kmsClient.
			On("Decrypt", &kms.DecryptInput{
				CiphertextBlob:    []byte("encrypted"),
				EncryptionContext: awskms.GetPrivateKeyEncryptionContext(distributionAccount.Address()),
				KeyId:             utils.PointOf("aws-arn"),
			}).
			Return(&kms.DecryptOutput{Plaintext: []byte(distributionAccount.Seed())}, nil).
			Once()
		defer kmsClient.AssertExpectations(t)

		signedFeeBumpTx, err := sc.SignStellarFeeBumpTransaction(ctx, feeBumpTx)
		require.NoError(t, err)
		assert.Equal(t, expectedSignedFeeBumpTx.Signatures(), signedFeeBumpTx.Signatures())
	})
}
