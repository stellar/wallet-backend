package services

import (
	"context"
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
)

func Test_ChannelAccountService_EnsureChannelAccounts(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, outerErr := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, outerErr)
	defer dbConnectionPool.Close()

	type testCase struct {
		name             string
		getCtx           func() context.Context
		numberOfAccounts int64
		setupMocks       func(t *testing.T, mockRPCService *RPCServiceMock, signatureClient *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock)
		expectedError    string
	}

	testCases := []testCase{
		{
			name:             "🟢sufficient_number_of_channel_accounts",
			numberOfAccounts: 5,
			getCtx:           context.Background,
			setupMocks: func(t *testing.T, _ *RPCServiceMock, _ *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock) {
				channelAccountStore.
					On("Count", mock.Anything).
					Return(5, nil).
					Once()
			},
		},
		{
			name:             "🟢successfully_ensures_the_channel_accounts_creation",
			numberOfAccounts: 5,
			getCtx:           context.Background,
			setupMocks: func(t *testing.T, mockRPCService *RPCServiceMock, signatureClient *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock) {
				channelAccountStore.
					On("Count", mock.Anything).
					Return(2, nil).
					Once()

				distributionAccount := keypair.MustRandom()
				channelAccountsAddressesBeingInserted := []string{}
				signedTx := txnbuild.Transaction{}
				signatureClient.
					On("GetAccountPublicKey", mock.Anything).
					Return(distributionAccount.Address(), nil).
					Once().
					On("SignStellarTransaction", mock.Anything, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
					Run(func(args mock.Arguments) {
						tx, ok := args.Get(1).(*txnbuild.Transaction)
						require.True(t, ok)

						assert.Equal(t, distributionAccount.Address(), tx.SourceAccount().AccountID)
						assert.Len(t, tx.Operations(), 3*3)

						for i := 0; i < len(tx.Operations()); i += 3 {
							ops := tx.Operations()[i : i+3]
							beginOp, ok := ops[0].(*txnbuild.BeginSponsoringFutureReserves)
							require.True(t, ok)
							assert.NotEqual(t, distributionAccount.Address(), beginOp.SponsoredID)

							createOp, ok := ops[1].(*txnbuild.CreateAccount)
							require.True(t, ok)
							assert.NotEqual(t, distributionAccount.Address(), createOp.Destination)
							assert.Equal(t, "0", createOp.Amount)

							endOp, ok := ops[2].(*txnbuild.EndSponsoringFutureReserves)
							require.True(t, ok)
							assert.NotEqual(t, distributionAccount.Address(), endOp.SourceAccount)

							channelAccountsAddressesBeingInserted = append(channelAccountsAddressesBeingInserted, createOp.Destination)
						}

						tx, err := tx.Sign(network.TestNetworkPassphrase, distributionAccount)
						require.NoError(t, err)

						signedTx = *tx
					}).
					Return(&signedTx, nil).
					Once().
					On("NetworkPassphrase").
					Return(network.TestNetworkPassphrase).
					Twice()

				heartbeatChan := make(chan entities.RPCGetHealthResult, 1)
				heartbeatChan <- entities.RPCGetHealthResult{Status: "healthy"}
				mockRPCService.
					On("GetHeartbeatChannel").
					Return(heartbeatChan).
					Once().
					On("GetAccountLedgerSequence", distributionAccount.Address()).
					Return(int64(123), nil).
					Once().
					On("SendTransaction", mock.AnythingOfType("string")).
					Return(entities.RPCSendTransactionResult{Status: entities.PendingStatus}, nil).
					Once().
					On("GetTransaction", mock.AnythingOfType("string")).
					Return(entities.RPCGetTransactionResult{Status: entities.SuccessStatus}, nil).
					Once()

				channelAccountStore.
					On("BatchInsert", mock.Anything, dbConnectionPool, mock.AnythingOfType("[]*store.ChannelAccount")).
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
			},
		},
		{
			name:             "🔴fails_when_transaction_submission_fails",
			numberOfAccounts: 5,
			getCtx:           context.Background,
			setupMocks: func(t *testing.T, mockRPCService *RPCServiceMock, signatureClient *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock) {
				channelAccountStore.
					On("Count", mock.Anything).
					Return(2, nil).
					Once()

				distributionAccount := keypair.MustRandom()
				signedTx := txnbuild.Transaction{}
				signatureClient.
					On("GetAccountPublicKey", mock.Anything).
					Return(distributionAccount.Address(), nil).
					Once().
					On("SignStellarTransaction", mock.Anything, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
					Run(func(args mock.Arguments) {
						tx, ok := args.Get(1).(*txnbuild.Transaction)
						require.True(t, ok)

						tx, err := tx.Sign(network.TestNetworkPassphrase, distributionAccount)
						require.NoError(t, err)

						signedTx = *tx
					}).
					Return(&signedTx, nil).
					Once().
					On("NetworkPassphrase").
					Return(network.TestNetworkPassphrase).
					Twice()

				heartbeatChan := make(chan entities.RPCGetHealthResult, 1)
				heartbeatChan <- entities.RPCGetHealthResult{Status: "healthy"}
				mockRPCService.
					On("GetHeartbeatChannel").
					Return(heartbeatChan).
					Once().
					On("GetAccountLedgerSequence", distributionAccount.Address()).
					Return(int64(123), nil).
					Once().
					On("SendTransaction", mock.AnythingOfType("string")).
					Return(entities.RPCSendTransactionResult{
						Status:         entities.ErrorStatus,
						ErrorResultXDR: "error_xdr",
					}, nil).
					Once()
			},
			expectedError: "failed with errorResultXdr error_xdr",
		},
		{
			name:             "🔴fails_when_transaction_status_check_fails",
			numberOfAccounts: 5,
			getCtx:           context.Background,
			setupMocks: func(t *testing.T, mockRPCService *RPCServiceMock, signatureClient *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock) {
				channelAccountStore.
					On("Count", mock.Anything).
					Return(2, nil).
					Once()

				distributionAccount := keypair.MustRandom()
				signedTx := txnbuild.Transaction{}
				signatureClient.
					On("GetAccountPublicKey", mock.Anything).
					Return(distributionAccount.Address(), nil).
					Once().
					On("SignStellarTransaction", mock.Anything, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
					Run(func(args mock.Arguments) {
						tx, ok := args.Get(1).(*txnbuild.Transaction)
						require.True(t, ok)

						tx, err := tx.Sign(network.TestNetworkPassphrase, distributionAccount)
						require.NoError(t, err)

						signedTx = *tx
					}).
					Return(&signedTx, nil).
					Once().
					On("NetworkPassphrase").
					Return(network.TestNetworkPassphrase).
					Twice()

				heartbeatChan := make(chan entities.RPCGetHealthResult, 1)
				heartbeatChan <- entities.RPCGetHealthResult{Status: "healthy"}
				mockRPCService.
					On("GetHeartbeatChannel").
					Return(heartbeatChan).
					Once().
					On("GetAccountLedgerSequence", distributionAccount.Address()).
					Return(int64(123), nil).
					Once().
					On("SendTransaction", mock.AnythingOfType("string")).
					Return(entities.RPCSendTransactionResult{Status: entities.PendingStatus}, nil).
					Once().
					On("GetTransaction", mock.AnythingOfType("string")).
					Return(entities.RPCGetTransactionResult{
						Status:         entities.FailedStatus,
						ErrorResultXDR: "error_xdr",
					}, nil).
					Once()
			},
			expectedError: "failed with status FAILED and errorResultXdr error_xdr",
		},
		{
			name:             "🔴fails_if_numOfChannelAccountsToCreate>MaximumCreateAccountOperationsPerStellarTx",
			numberOfAccounts: MaximumCreateAccountOperationsPerStellarTx + 1,
			getCtx:           context.Background,
			setupMocks: func(t *testing.T, _ *RPCServiceMock, _ *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock) {
				channelAccountStore.
					On("Count", mock.Anything).
					Return(0, nil).
					Once()
			},
			expectedError: "number of channel accounts to create is greater than the maximum allowed per transaction (19)",
		},
		{
			name:             "🔴fails_if_rpc_service_is_not_healthy",
			numberOfAccounts: 5,
			getCtx:           func() context.Context { ctx, cancel := context.WithCancel(context.Background()); cancel(); return ctx },
			setupMocks: func(t *testing.T, mockRPCService *RPCServiceMock, signatureClient *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock) {
				channelAccountStore.
					On("Count", mock.Anything).
					Return(2, nil).
					Once()

				distributionAccount := keypair.MustRandom()
				signatureClient.
					On("GetAccountPublicKey", mock.Anything).
					Return(distributionAccount.Address(), nil).
					Once()

				heartbeatChan := make(chan entities.RPCGetHealthResult, 1)
				mockRPCService.On("GetHeartbeatChannel").Return(heartbeatChan)
			},
			expectedError: "context cancelled while waiting for rpc service to become healthy",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create fresh mocks for each test
			ctx := tc.getCtx()
			mockRPCService := NewRPCServiceMock(t)
			mockRPCService.On("TrackRPCServiceHealth", ctx, mock.Anything).Return()
			signatureClient := signing.NewSignatureClientMock(t)
			channelAccountStore := store.NewChannelAccountStoreMock(t)

			// Setup mocks
			tc.setupMocks(t, mockRPCService, signatureClient, channelAccountStore)

			// Create service with fresh mocks
			s, err := NewChannelAccountService(ctx, ChannelAccountServiceOptions{
				DB:                                 dbConnectionPool,
				RPCService:                         mockRPCService,
				BaseFee:                            txnbuild.MinBaseFee,
				DistributionAccountSignatureClient: signatureClient,
				ChannelAccountStore:                channelAccountStore,
				PrivateKeyEncrypter:                &signingutils.DefaultPrivateKeyEncrypter{},
				EncryptionPassphrase:               "my-encryption-passphrase",
			})
			time.Sleep(50 * time.Millisecond) // waiting for the goroutine to call `TrackRPCServiceHealth`
			require.NoError(t, err)

			// Execute test
			err = s.EnsureChannelAccounts(ctx, tc.numberOfAccounts)

			// Assert expectations
			if tc.expectedError != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSubmitTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	mockRPCService := RPCServiceMock{}
	mockRPCService.On("TrackRPCServiceHealth", ctx, mock.Anything).Return()
	defer mockRPCService.AssertExpectations(t)
	signatureClient := signing.SignatureClientMock{}
	channelAccountStore := store.ChannelAccountStoreMock{}
	privateKeyEncrypter := signingutils.DefaultPrivateKeyEncrypter{}
	passphrase := "test"
	s, err := NewChannelAccountService(ctx, ChannelAccountServiceOptions{
		DB:                                 dbConnectionPool,
		RPCService:                         &mockRPCService,
		BaseFee:                            100 * txnbuild.MinBaseFee,
		DistributionAccountSignatureClient: &signatureClient,
		ChannelAccountStore:                &channelAccountStore,
		PrivateKeyEncrypter:                &privateKeyEncrypter,
		EncryptionPassphrase:               passphrase,
	})
	time.Sleep(100 * time.Millisecond) // waiting for the goroutine to call `TrackRPCServiceHealth`
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond) // waiting for the goroutine to call `TrackRPCServiceHealth`

	hash := "test_hash"
	signedTxXDR := "test_xdr"

	t.Run("successful_pending", func(t *testing.T) {
		mockRPCService.
			On("SendTransaction", signedTxXDR).
			Return(entities.RPCSendTransactionResult{Status: entities.PendingStatus}, nil).
			Once()

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

		err := s.submitTransaction(ctx, hash, signedTxXDR)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed with errorResultXdr error_xdr")
	})
}

func TestWaitForTransactionConfirmation(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	mockRPCService := RPCServiceMock{}
	defer mockRPCService.AssertExpectations(t)
	mockRPCService.On("TrackRPCServiceHealth", ctx, mock.Anything).Return()
	signatureClient := signing.SignatureClientMock{}
	channelAccountStore := store.ChannelAccountStoreMock{}
	privateKeyEncrypter := signingutils.DefaultPrivateKeyEncrypter{}
	passphrase := "test"
	s, err := NewChannelAccountService(ctx, ChannelAccountServiceOptions{
		DB:                                 dbConnectionPool,
		RPCService:                         &mockRPCService,
		BaseFee:                            100 * txnbuild.MinBaseFee,
		DistributionAccountSignatureClient: &signatureClient,
		ChannelAccountStore:                &channelAccountStore,
		PrivateKeyEncrypter:                &privateKeyEncrypter,
		EncryptionPassphrase:               passphrase,
	})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond) // waiting for the goroutine to call `TrackRPCServiceHealth`

	hash := "test_hash"

	t.Run("successful", func(t *testing.T) {
		mockRPCService.
			On("GetTransaction", hash).
			Return(entities.RPCGetTransactionResult{Status: entities.SuccessStatus}, nil).
			Once()

		err := s.waitForTransactionConfirmation(ctx, hash)
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

		err := s.waitForTransactionConfirmation(ctx, hash)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed with status FAILED and errorResultXdr error_xdr")
	})
}
