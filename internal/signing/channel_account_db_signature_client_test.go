package signing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
)

func TestChannelAccountDBSignatureClientGetAccountPublicKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	retryCount := 6
	retryInterval := 100 * time.Millisecond
	privateKeyEncrypter := signingutils.DefaultPrivateKeyEncrypter{}
	channelAccountStore := store.ChannelAccountStoreMock{}
	sc := channelAccountDBSignatureClient{
		networkPassphrase:    network.TestNetworkPassphrase,
		encryptionPassphrase: "test",
		privateKeyEncrypter:  &privateKeyEncrypter,
		channelAccountStore:  &channelAccountStore,
		retryCount:           retryCount,
		retryInterval:        retryInterval,
	}

	t.Run("returns_error_when_couldn't_get_an_idle_channel_account", func(t *testing.T) {
		channelAccountStore.
			On("GetAndLockIdleChannelAccount", ctx, time.Duration(100)*time.Second).
			Return(nil, store.ErrNoIdleChannelAccountAvailable).
			Times(retryCount).
			On("Count", ctx).
			Return(retryCount-1, nil).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		getEntries := log.DefaultLogger.StartTest(log.WarnLevel)

		publicKey, err := sc.GetAccountPublicKey(ctx, 100)
		assert.ErrorIs(t, err, store.ErrNoIdleChannelAccountAvailable)
		assert.Empty(t, publicKey)

		entries := getEntries()
		require.Len(t, entries, retryCount)

		for _, entry := range entries {
			assert.Equal(t, entry.Message, fmt.Sprintf("All channel accounts are in use. Retry in %s.", retryInterval))
		}
	})

	t.Run("returns_error_when_there's_no_channel_account_configured", func(t *testing.T) {
		channelAccountStore.
			On("GetAndLockIdleChannelAccount", ctx, time.Minute).
			Return(nil, store.ErrNoIdleChannelAccountAvailable).
			Times(retryCount).
			On("Count", ctx).
			Return(0, nil).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		getEntries := log.DefaultLogger.StartTest(log.WarnLevel)

		publicKey, err := sc.GetAccountPublicKey(ctx)
		assert.ErrorIs(t, err, store.ErrNoChannelAccountConfigured)
		assert.Empty(t, publicKey)

		entries := getEntries()
		require.Len(t, entries, retryCount)

		for _, entry := range entries {
			assert.Equal(t, fmt.Sprintf("All channel accounts are in use. Retry in %s.", retryInterval), entry.Message)
		}
	})

	t.Run("gets_an_idle_channel_account", func(t *testing.T) {
		channelAccountPublicKey := keypair.MustRandom().Address()
		channelAccountStore.
			On("GetAndLockIdleChannelAccount", ctx, time.Minute).
			Return(&store.ChannelAccount{PublicKey: channelAccountPublicKey}, nil).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		publicKey, err := sc.GetAccountPublicKey(ctx)
		require.NoError(t, err)
		assert.Equal(t, channelAccountPublicKey, publicKey)
	})
}

func TestChannelAccountDBSignatureNetworkPassphrase(t *testing.T) {
	sc := channelAccountDBSignatureClient{networkPassphrase: network.PublicNetworkPassphrase}
	assert.Equal(t, network.PublicNetworkPassphrase, sc.NetworkPassphrase())
}

func TestChannelAccountDBSignatureClientSignStellarTransaction(t *testing.T) {
	ctx := context.Background()
	privateKeyEncrypter := signingutils.DefaultPrivateKeyEncrypter{}
	channelAccountStore := store.ChannelAccountStoreMock{}
	passphrase := "test"
	sc := channelAccountDBSignatureClient{
		networkPassphrase:    network.TestNetworkPassphrase,
		encryptionPassphrase: passphrase,
		privateKeyEncrypter:  &privateKeyEncrypter,
		channelAccountStore:  &channelAccountStore,
	}

	t.Run("invalid_transaction", func(t *testing.T) {
		signedTx, err := sc.SignStellarTransaction(ctx, nil)
		assert.ErrorIs(t, err, ErrInvalidTransaction)
		assert.Nil(t, signedTx)
	})

	t.Run("signs_transaction_successfully", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		sourceAccount := txnbuild.NewSimpleAccount(channelAccount.Address(), int64(9605939170639897))
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
		expectedSignedTx, err := tx.Sign(network.TestNetworkPassphrase, channelAccount)
		require.NoError(t, err)

		encryptedPrivateKey, err := privateKeyEncrypter.Encrypt(ctx, channelAccount.Seed(), passphrase)
		require.NoError(t, err)

		chAcc := store.ChannelAccount{
			PublicKey:           channelAccount.Address(),
			EncryptedPrivateKey: encryptedPrivateKey,
		}
		channelAccountStore.
			On("GetAllByPublicKey", ctx, nil, []string{channelAccount.Address()}).
			Return([]*store.ChannelAccount{&chAcc}, nil).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		signedTx, err := sc.SignStellarTransaction(ctx, tx, channelAccount.Address())
		require.NoError(t, err)
		assert.Equal(t, expectedSignedTx.Signatures(), signedTx.Signatures())
	})
}
