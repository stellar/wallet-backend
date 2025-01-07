package services

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
	"github.com/stellar/wallet-backend/internal/tss/router"
)

func TestChannelAccountServiceEnsureChannelAccounts(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	mockRouter := router.MockRouter{}
	mockRPCService := RPCServiceMock{}
	signatureClient := signing.SignatureClientMock{}
	channelAccountStore := store.ChannelAccountStoreMock{}
	privateKeyEncrypter := signingutils.DefaultPrivateKeyEncrypter{}
	passphrase := "test"
	s, err := NewChannelAccountService(ChannelAccountServiceOptions{
		DB:                                 dbConnectionPool,
		RPCService:                         &mockRPCService,
		BaseFee:                            100 * txnbuild.MinBaseFee,
		Router:                             &mockRouter,
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

		mockRPCService.
			On("GetHealth").
			Return(entities.RPCGetHealthResult{Status: "healthy"}, nil)

		mockRPCService.
			On("GetAccountLedgerSequence", distributionAccount.Address()).
			Return(int64(123), nil).
			Once()

		mockRPCService.
			On("SendTransaction", mock.AnythingOfType("string")).
			Return(entities.RPCSendTransactionResult{Status: entities.PendingStatus}, nil).
			Once()

		mockRPCService.
			On("GetTransaction", mock.AnythingOfType("string")).
			Return(entities.RPCGetTransactionResult{Status: entities.SuccessStatus}, nil).
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

		err = s.EnsureChannelAccounts(ctx, 5)
		require.NoError(t, err)
	})

	t.Run("fails_when_transaction_submission_fails", func(t *testing.T) {
		channelAccountStore.
			On("Count", ctx).
			Return(2, nil).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		distributionAccount := keypair.MustRandom()
		signedTx := txnbuild.Transaction{}
		signatureClient.
			On("GetAccountPublicKey", ctx).
			Return(distributionAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
			Run(func(args mock.Arguments) {
				tx, ok := args.Get(1).(*txnbuild.Transaction)
				require.True(t, ok)

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

		mockRPCService.
			On("GetHealth").
			Return(entities.RPCGetHealthResult{Status: "healthy"}, nil)

		mockRPCService.
			On("GetAccountLedgerSequence", distributionAccount.Address()).
			Return(int64(123), nil).
			Once()

		mockRPCService.
			On("SendTransaction", mock.AnythingOfType("string")).
			Return(entities.RPCSendTransactionResult{
				Status:         entities.ErrorStatus,
				ErrorResultXDR: "error_xdr",
			}, nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		err = s.EnsureChannelAccounts(ctx, 5)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transaction failed error_xdr")
	})

	t.Run("fails_when_transaction_status_check_fails", func(t *testing.T) {
		channelAccountStore.
			On("Count", ctx).
			Return(2, nil).
			Once()
		defer channelAccountStore.AssertExpectations(t)

		distributionAccount := keypair.MustRandom()
		signedTx := txnbuild.Transaction{}
		signatureClient.
			On("GetAccountPublicKey", ctx).
			Return(distributionAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
			Run(func(args mock.Arguments) {
				tx, ok := args.Get(1).(*txnbuild.Transaction)
				require.True(t, ok)

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

		mockRPCService.
			On("GetHealth").
			Return(entities.RPCGetHealthResult{Status: "healthy"}, nil)

		mockRPCService.
			On("GetAccountLedgerSequence", distributionAccount.Address()).
			Return(int64(123), nil).
			Once()

		mockRPCService.
			On("SendTransaction", mock.AnythingOfType("string")).
			Return(entities.RPCSendTransactionResult{Status: entities.PendingStatus}, nil).
			Once()

		mockRPCService.
			On("GetTransaction", mock.AnythingOfType("string")).
			Return(entities.RPCGetTransactionResult{
				Status:         entities.FailedStatus,
				ErrorResultXDR: "error_xdr",
			}, nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		err = s.EnsureChannelAccounts(ctx, 5)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transaction failed")
	})
}

func TestWaitForRPCServiceHealth(t *testing.T) {
	mockRPCService := RPCServiceMock{}
	ctx := context.Background()

	t.Run("successful", func(t *testing.T) {
		mockRPCService.
			On("GetHealth").
			Return(entities.RPCGetHealthResult{Status: "healthy"}, nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		err := waitForRPCServiceHealth(ctx, &mockRPCService)
		require.NoError(t, err)
	})

	t.Run("context_cancelled", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		mockRPCService.
			On("GetHealth").
			Return(entities.RPCGetHealthResult{}, fmt.Errorf("connection failed"))
		defer mockRPCService.AssertExpectations(t)

		err := waitForRPCServiceHealth(ctx, &mockRPCService)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "context cancelled")
	})
}

func TestSubmitTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockRPCService := RPCServiceMock{}
	mockRouter := router.MockRouter{}
	signatureClient := signing.SignatureClientMock{}
	channelAccountStore := store.ChannelAccountStoreMock{}
	privateKeyEncrypter := signingutils.DefaultPrivateKeyEncrypter{}
	passphrase := "test"
	s, err := NewChannelAccountService(ChannelAccountServiceOptions{
		DB:                                 dbConnectionPool,
		RPCService:                         &mockRPCService,
		BaseFee:                            100 * txnbuild.MinBaseFee,
		Router:                             &mockRouter,
		DistributionAccountSignatureClient: &signatureClient,
		ChannelAccountStore:                &channelAccountStore,
		PrivateKeyEncrypter:                &privateKeyEncrypter,
		EncryptionPassphrase:               passphrase,
	})
	require.NoError(t, err)

	ctx := context.Background()
	hash := "test_hash"
	signedTxXDR := "test_xdr"

	t.Run("successful_pending", func(t *testing.T) {
		mockRPCService.
			On("SendTransaction", signedTxXDR).
			Return(entities.RPCSendTransactionResult{Status: entities.PendingStatus}, nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		err := s.submitTransaction(ctx, hash, signedTxXDR)
		require.NoError(t, err)
	})

	t.Run("error_status", func(t *testing.T) {
		mockRPCService.
			On("SendTransaction", signedTxXDR).
			Return(entities.RPCSendTransactionResult{
				Status:         entities.ErrorStatus,
				ErrorResultXDR: "error_xdr",
			}, nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		err := s.submitTransaction(ctx, hash, signedTxXDR)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transaction failed error_xdr")
	})
}

func TestGetTransactionStatus(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockRPCService := RPCServiceMock{}
	mockRouter := router.MockRouter{}
	signatureClient := signing.SignatureClientMock{}
	channelAccountStore := store.ChannelAccountStoreMock{}
	privateKeyEncrypter := signingutils.DefaultPrivateKeyEncrypter{}
	passphrase := "test"
	s, err := NewChannelAccountService(ChannelAccountServiceOptions{
		DB:                                 dbConnectionPool,
		RPCService:                         &mockRPCService,
		BaseFee:                            100 * txnbuild.MinBaseFee,
		Router:                             &mockRouter,
		DistributionAccountSignatureClient: &signatureClient,
		ChannelAccountStore:                &channelAccountStore,
		PrivateKeyEncrypter:                &privateKeyEncrypter,
		EncryptionPassphrase:               passphrase,
	})
	require.NoError(t, err)

	ctx := context.Background()
	hash := "test_hash"

	t.Run("successful", func(t *testing.T) {
		mockRPCService.
			On("GetTransaction", hash).
			Return(entities.RPCGetTransactionResult{Status: entities.SuccessStatus}, nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		err := s.getTransactionStatus(ctx, hash)
		require.NoError(t, err)
	})

	t.Run("failed_status", func(t *testing.T) {
		mockRPCService.
			On("GetTransaction", hash).
			Return(entities.RPCGetTransactionResult{
				Status:         entities.FailedStatus,
				ErrorResultXDR: "error_xdr",
			}, nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		err := s.getTransactionStatus(ctx, hash)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transaction failed")
	})
}
