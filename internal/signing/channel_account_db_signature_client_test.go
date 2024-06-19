package signing

import (
	"context"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/signing/channelaccounts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannelAccountDBSignatureClientGetAccountPublicKey(t *testing.T) {
	ctx := context.Background()
	privateKeyEncrypter := channelaccounts.DefaultPrivateKeyEncrypter{}
	channelAccountStore := channelaccounts.ChannelAccountStoreMock{}
	sc := channelAccountDBSignatureClient{
		networkPassphrase:    network.TestNetworkPassphrase,
		encryptionPassphrase: "test",
		privateKeyEncrypter:  &privateKeyEncrypter,
		channelAccountStore:  &channelAccountStore,
	}

	t.Run("returns_error_when_couldn't_get_an_idle_channel_account", func(t *testing.T) {
		channelAccountStore.
			On("GetIdleChannelAccount", ctx, time.Minute).
			Return(nil, channelaccounts.ErrNoAvailableChannelAccount).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		publicKey, err := sc.GetAccountPublicKey(ctx)
		assert.ErrorIs(t, err, channelaccounts.ErrNoAvailableChannelAccount)
		assert.Empty(t, publicKey)
	})

	t.Run("gets_an_idle_channel_account", func(t *testing.T) {
		channelAccountPublicKey := keypair.MustRandom().Address()
		channelAccountStore.
			On("GetIdleChannelAccount", ctx, time.Minute).
			Return(&channelaccounts.ChannelAccount{PublicKey: channelAccountPublicKey}, nil).
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
	privateKeyEncrypter := channelaccounts.DefaultPrivateKeyEncrypter{}
	channelAccountStore := channelaccounts.ChannelAccountStoreMock{}
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

		chAcc := channelaccounts.ChannelAccount{
			PublicKey:           channelAccount.Address(),
			EncryptedPrivateKey: encryptedPrivateKey,
		}
		channelAccountStore.
			On("Get", ctx, nil, channelAccount.Address()).
			Return(&chAcc, nil).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		signedTx, err := sc.SignStellarTransaction(ctx, tx, channelAccount.Address())
		require.NoError(t, err)
		assert.Equal(t, expectedSignedTx.Signatures(), signedTx.Signatures())
	})
}
