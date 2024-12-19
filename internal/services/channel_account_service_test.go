package services

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/support/render/problem"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
)

func TestChannelAccountServiceEnsureChannelAccounts(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	horizonClient := horizonclient.MockClient{}
	mockRPCService := RPCServiceMock{}
	signatureClient := signing.SignatureClientMock{}
	channelAccountStore := store.ChannelAccountStoreMock{}
	privateKeyEncrypter := signingutils.DefaultPrivateKeyEncrypter{}
	passphrase := "test"
	s, err := NewChannelAccountService(ChannelAccountServiceOptions{
		DB:                                 dbConnectionPool,
		HorizonClient:                      &horizonClient,
		RPCService:                         &mockRPCService,
		BaseFee:                            100 * txnbuild.MinBaseFee,
		DistributionAccountSignatureClient: &signatureClient,
		ChannelAccountStore:                &channelAccountStore,
		PrivateKeyEncrypter:                &privateKeyEncrypter,
		EncryptionPassphrase:               passphrase,
	})
	require.NoError(t, err)

	t.Run("sufficient_number_of_channel_accounts", func(t *testing.T) {
		channelAccountStore.
			On("Count", ctx).
			Return(5, nil).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		err := s.EnsureChannelAccounts(ctx, 5)
		require.NoError(t, err)
	})

	t.Run("horizon_timeout", func(t *testing.T) {
		channelAccountStore.
			On("Count", ctx).
			Return(2, nil).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		distributionAccount := keypair.MustRandom()
		channelAccountsAddressesBeingInserted := []string{}
		signedTx := txnbuild.Transaction{}
		signatureClient.
			On("GetAccountPublicKey", ctx).
			Return(distributionAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
			Run(func(args mock.Arguments) {
				tx, ok := args.Get(1).(*txnbuild.Transaction)
				require.True(t, ok)

				assert.Equal(t, distributionAccount.Address(), tx.SourceAccount().AccountID)
				assert.Len(t, tx.Operations(), 3)

				for _, op := range tx.Operations() {
					caOp, ok := op.(*txnbuild.CreateAccount)
					require.True(t, ok)

					assert.Equal(t, "1", caOp.Amount)
					assert.Equal(t, distributionAccount.Address(), caOp.SourceAccount)
					channelAccountsAddressesBeingInserted = append(channelAccountsAddressesBeingInserted, caOp.Destination)
				}

				tx, err = tx.Sign(network.TestNetworkPassphrase, distributionAccount)
				require.NoError(t, err)

				signedTx = *tx
			}).
			Return(&signedTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(network.TestNetworkPassphrase).
			Once()
		defer signatureClient.AssertExpectations(t)

		horizonClient.
			On("SubmitTransaction", mock.AnythingOfType("*txnbuild.Transaction")).
			Return(horizon.Transaction{}, horizonclient.Error{
				Response: &http.Response{},
				Problem: problem.P{
					Type:   "https://stellar.org/horizon-errors/timeout",
					Status: http.StatusRequestTimeout,
					Detail: "Timeout",
					Extras: map[string]interface{}{},
				},
			}).
			Once()
		defer horizonClient.AssertExpectations(t)

		mockRPCService.
			On("GetAccountLedgerSequence", distributionAccount.Address()).
			Return(int64(123), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		err := s.EnsureChannelAccounts(ctx, 5)
		require.Error(t, err)

		txHash, hashErr := signedTx.HashHex(network.TestNetworkPassphrase)
		require.NoError(t, hashErr)
		assert.EqualError(t, err, fmt.Sprintf("submitting create channel accounts on chain transaction: horizon request timed out while creating a channel account. Transaction hash: %s", txHash))
	})

	t.Run("horizon_bad_request", func(t *testing.T) {
		channelAccountStore.
			On("Count", ctx).
			Return(2, nil).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		distributionAccount := keypair.MustRandom()
		channelAccountsAddressesBeingInserted := []string{}
		signedTx := txnbuild.Transaction{}
		signatureClient.
			On("GetAccountPublicKey", ctx).
			Return(distributionAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
			Run(func(args mock.Arguments) {
				tx, ok := args.Get(1).(*txnbuild.Transaction)
				require.True(t, ok)

				assert.Equal(t, distributionAccount.Address(), tx.SourceAccount().AccountID)
				assert.Len(t, tx.Operations(), 3)

				for _, op := range tx.Operations() {
					caOp, ok := op.(*txnbuild.CreateAccount)
					require.True(t, ok)

					assert.Equal(t, "1", caOp.Amount)
					assert.Equal(t, distributionAccount.Address(), caOp.SourceAccount)
					channelAccountsAddressesBeingInserted = append(channelAccountsAddressesBeingInserted, caOp.Destination)
				}

				tx, err = tx.Sign(network.TestNetworkPassphrase, distributionAccount)
				require.NoError(t, err)

				signedTx = *tx
			}).
			Return(&signedTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(network.TestNetworkPassphrase).
			Once()
		defer signatureClient.AssertExpectations(t)

		horizonClient.
			On("SubmitTransaction", mock.AnythingOfType("*txnbuild.Transaction")).
			Return(horizon.Transaction{}, horizonclient.Error{
				Response: &http.Response{},
				Problem: problem.P{
					Title:  "Some bad request error",
					Status: http.StatusBadRequest,
					Detail: "Bad Request",
					Extras: map[string]interface{}{},
				},
			}).
			Once()
		defer horizonClient.AssertExpectations(t)

		mockRPCService.
			On("GetAccountLedgerSequence", distributionAccount.Address()).
			Return(int64(123), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		err := s.EnsureChannelAccounts(ctx, 5)
		assert.EqualError(t, err, `submitting create channel accounts on chain transaction: submitting transaction: Type: , Title: Some bad request error, Status: 400, Detail: Bad Request, Extras: map[]: horizon error: "Some bad request error" - check horizon.Error.Problem for more information`)
	})

	t.Run("successfully_ensures_the_channel_accounts_creation", func(t *testing.T) {
		channelAccountStore.
			On("Count", ctx).
			Return(2, nil).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		distributionAccount := keypair.MustRandom()
		channelAccountsAddressesBeingInserted := []string{}
		signedTx := txnbuild.Transaction{}
		signatureClient.
			On("GetAccountPublicKey", ctx).
			Return(distributionAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
			Run(func(args mock.Arguments) {
				tx, ok := args.Get(1).(*txnbuild.Transaction)
				require.True(t, ok)

				assert.Equal(t, distributionAccount.Address(), tx.SourceAccount().AccountID)
				assert.Len(t, tx.Operations(), 3)

				for _, op := range tx.Operations() {
					caOp, ok := op.(*txnbuild.CreateAccount)
					require.True(t, ok)

					assert.Equal(t, "1", caOp.Amount)
					assert.Equal(t, distributionAccount.Address(), caOp.SourceAccount)
					channelAccountsAddressesBeingInserted = append(channelAccountsAddressesBeingInserted, caOp.Destination)
				}

				tx, err = tx.Sign(network.TestNetworkPassphrase, distributionAccount)
				require.NoError(t, err)

				signedTx = *tx
			}).
			Return(&signedTx, nil).
			Once().
			On("NetworkPassphrase").
			Return(network.TestNetworkPassphrase).
			Once()
		defer signatureClient.AssertExpectations(t)

		horizonClient.
			On("SubmitTransaction", mock.AnythingOfType("*txnbuild.Transaction")).
			Return(horizon.Transaction{}, nil).
			Once()
		defer horizonClient.AssertExpectations(t)

		mockRPCService.
			On("GetAccountLedgerSequence", distributionAccount.Address()).
			Return(int64(123), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		channelAccountStore.
			On("BatchInsert", ctx, dbConnectionPool, mock.AnythingOfType("[]*store.ChannelAccount")).
			Run(func(args mock.Arguments) {
				channelAccounts, ok := args.Get(2).([]*store.ChannelAccount)
				require.True(t, ok)

				channelAccountsAddresses := make([]string, 0, len(channelAccounts))
				for _, ca := range channelAccounts {
					channelAccountsAddresses = append(channelAccountsAddresses, ca.PublicKey)
				}

				assert.Equal(t, channelAccountsAddressesBeingInserted, channelAccountsAddresses)
			}).
			Return(nil).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		err := s.EnsureChannelAccounts(ctx, 5)
		require.NoError(t, err)
	})
}
