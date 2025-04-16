package services

import (
	"context"
	"testing"
	"time"

	"github.com/avast/retry-go/v4"
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
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	distributionAccount := keypair.MustRandom()

	testCases := []struct {
		name            string
		cancelContext   bool
		prepareMocks    func(mRPCService *RPCServiceMock, mChannelAccountStore *store.ChannelAccountStoreMock, mSignatureClient *signing.SignatureClientMock)
		wantErrContains []string
	}{
		{
			name: "sufficient_number_of_channel_accounts",
			prepareMocks: func(_ *RPCServiceMock, mChannelAccountStore *store.ChannelAccountStoreMock, _ *signing.SignatureClientMock) {
				mChannelAccountStore.
					On("Count", mock.Anything).
					Return(5, nil).
					Once()
			},
		},
		{
			name: "successfully_ensures_the_channel_accounts_creation",
			prepareMocks: func(mRPCService *RPCServiceMock, mChannelAccountStore *store.ChannelAccountStoreMock, mSignatureClient *signing.SignatureClientMock) {
				mChannelAccountStore.
					On("Count", mock.Anything).
					Return(2, nil).
					Once()

				channelAccountsAddressesBeingInserted := []string{}
				signedTx := txnbuild.Transaction{}
				mSignatureClient.
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

						tx, err = tx.Sign(network.TestNetworkPassphrase, distributionAccount)
						require.NoError(t, err)

						signedTx = *tx
					}).
					Return(&signedTx, nil).
					Once().
					On("NetworkPassphrase").
					Return(network.TestNetworkPassphrase)

				// Create and set up the heartbeat channel
				heartbeatChan := make(chan entities.RPCGetHealthResult, 1)
				heartbeatChan <- entities.RPCGetHealthResult{Status: "healthy"}
				mRPCService.
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

				mChannelAccountStore.
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
			name: "fails_when_transaction_submission_fails",
			prepareMocks: func(mRPCService *RPCServiceMock, mChannelAccountStore *store.ChannelAccountStoreMock, mSignatureClient *signing.SignatureClientMock) {
				mChannelAccountStore.
					On("Count", mock.Anything).
					Return(2, nil).
					Once()

				signedTx := txnbuild.Transaction{}
				mSignatureClient.
					On("GetAccountPublicKey", mock.Anything).
					Return(distributionAccount.Address(), nil).
					Once().
					On("SignStellarTransaction", mock.Anything, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
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
					Return(network.TestNetworkPassphrase)

				heartbeatChan := make(chan entities.RPCGetHealthResult, 1)
				heartbeatChan <- entities.RPCGetHealthResult{Status: "healthy"}
				mRPCService.
					On("GetHeartbeatChannel").
					Return(heartbeatChan).
					Once().
					On("GetAccountLedgerSequence", distributionAccount.Address()).
					Return(int64(123), nil).
					Once().
					On("SendTransaction", mock.AnythingOfType("string")).
					Return(entities.RPCSendTransactionResult{
						Status:         entities.ErrorStatus,
						ErrorResultXDR: "custom_error_xdr",
					}, nil).
					Once()
			},
			wantErrContains: []string{"transaction with hash", "failed with errorResultXdr custom_error_xdr"},
		},
		{
			name: "fails_when_transaction_status_check_fails",
			prepareMocks: func(mRPCService *RPCServiceMock, mChannelAccountStore *store.ChannelAccountStoreMock, mSignatureClient *signing.SignatureClientMock) {
				mChannelAccountStore.
					On("Count", mock.Anything).
					Return(2, nil).
					Once()

				signedTx := txnbuild.Transaction{}
				mSignatureClient.
					On("GetAccountPublicKey", mock.Anything).
					Return(distributionAccount.Address(), nil).
					Once().
					On("SignStellarTransaction", mock.Anything, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
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
					Return(network.TestNetworkPassphrase)

				heartbeatChan := make(chan entities.RPCGetHealthResult, 1)
				heartbeatChan <- entities.RPCGetHealthResult{Status: "healthy"}
				mRPCService.
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
			wantErrContains: []string{"failed with status FAILED and errorResultXdr error_xdr"},
		},
		{
			name:          "fails_if_rpc_service_is_not_healthy",
			cancelContext: true,
			prepareMocks: func(mRPCService *RPCServiceMock, mChannelAccountStore *store.ChannelAccountStoreMock, mSignatureClient *signing.SignatureClientMock) {
				mChannelAccountStore.
					On("Count", mock.Anything).
					Return(2, nil).
					Once()

				mRPCService.
					On("GetAccountLedgerSequence", distributionAccount.Address()).
					Return(int64(123), nil).
					Once()
				heartbeatChan := make(chan entities.RPCGetHealthResult, 1)
				mRPCService.
					On("GetHeartbeatChannel").
					Return(heartbeatChan).
					Once()

				signedTx := txnbuild.Transaction{}
				mSignatureClient.
					On("GetAccountPublicKey", mock.Anything).
					Return(distributionAccount.Address(), nil).
					Once().
					On("SignStellarTransaction", mock.Anything, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
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
					Return(network.TestNetworkPassphrase)
			},
			wantErrContains: []string{"context cancelled while waiting for rpc service to become healthy"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mRPCService := NewRPCServiceMock(t)
			mRPCService.
				On("TrackRPCServiceHealth", mock.Anything).
				Return().
				Once()
			mChannelAccountStore := store.NewChannelAccountStoreMock(t)
			mSignatureClient := signing.NewSignatureClientMock(t)

			if tc.prepareMocks != nil {
				tc.prepareMocks(mRPCService, mChannelAccountStore, mSignatureClient)
			}

			ctx := context.Background()
			if tc.cancelContext {
				cancelCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
				ctx = cancelCtx
			}

			s, err := NewChannelAccountService(ctx, ChannelAccountServiceOptions{
				DB:                                 dbConnectionPool,
				RPCService:                         mRPCService,
				BaseFee:                            100 * txnbuild.MinBaseFee,
				DistributionAccountSignatureClient: mSignatureClient,
				ChannelAccountStore:                mChannelAccountStore,
				PrivateKeyEncrypter:                &signingutils.DefaultPrivateKeyEncrypter{},
				EncryptionPassphrase:               "test",
			})
			require.NoError(t, err)

			err = s.EnsureChannelAccounts(ctx, 5)
			if tc.wantErrContains == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)

				for _, wantErrContains := range tc.wantErrContains {
					assert.ErrorContains(t, err, wantErrContains)
				}
			}
		})
	}
}

func Test_ChannelAccountService_submitTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	hash := "test_hash"
	signedTxXDR := "test_xdr"

	testCases := []struct {
		name            string
		prepareMocks    func(mRPCService *RPCServiceMock, mSignatureClient *signing.SignatureClientMock, mChannelAccountStore *store.ChannelAccountStoreMock)
		wantErrContains []string
	}{
		{
			name: "🟢successful_response",
			prepareMocks: func(mRPCService *RPCServiceMock, mSignatureClient *signing.SignatureClientMock, mChannelAccountStore *store.ChannelAccountStoreMock) {
				mRPCService.
					On("SendTransaction", signedTxXDR).
					Return(entities.RPCSendTransactionResult{Status: entities.PendingStatus}, nil).
					Once()
			},
		},
		{
			name: "🔴failed_response",
			prepareMocks: func(mRPCService *RPCServiceMock, mSignatureClient *signing.SignatureClientMock, mChannelAccountStore *store.ChannelAccountStoreMock) {
				mRPCService.
					On("SendTransaction", signedTxXDR).
					Return(entities.RPCSendTransactionResult{
						Status:         entities.ErrorStatus,
						ErrorResultXDR: "error_xdr",
					}, nil).
					Once()
			},
			wantErrContains: []string{"transaction with hash", "test_hash", "failed with errorResultXdr error_xdr"},
		},
		{
			name: "🔄🟢successful_response_with_retry",
			prepareMocks: func(mRPCService *RPCServiceMock, mSignatureClient *signing.SignatureClientMock, mChannelAccountStore *store.ChannelAccountStoreMock) {
				mRPCService.
					On("SendTransaction", signedTxXDR).
					Return(entities.RPCSendTransactionResult{Status: entities.TryAgainLaterStatus}, nil).
					Once().
					On("SendTransaction", signedTxXDR).
					Return(entities.RPCSendTransactionResult{Status: entities.PendingStatus}, nil).
					Once()
			},
		},
		{
			name: "🔄🔴failed_response_with_retry",
			prepareMocks: func(mRPCService *RPCServiceMock, mSignatureClient *signing.SignatureClientMock, mChannelAccountStore *store.ChannelAccountStoreMock) {
				mRPCService.
					On("SendTransaction", signedTxXDR).
					Return(entities.RPCSendTransactionResult{Status: entities.TryAgainLaterStatus}, nil).
					Once().
					On("SendTransaction", signedTxXDR).
					Return(entities.RPCSendTransactionResult{
						Status:         entities.ErrorStatus,
						ErrorResultXDR: "error_xdr",
					}, nil).
					Once()
			},
			wantErrContains: []string{"transaction with hash", "test_hash", "failed with errorResultXdr error_xdr"},
		},
		{
			name: "🔴max_attempts_reached",
			prepareMocks: func(mRPCService *RPCServiceMock, mSignatureClient *signing.SignatureClientMock, mChannelAccountStore *store.ChannelAccountStoreMock) {
				mRPCService.
					On("SendTransaction", signedTxXDR).
					Return(entities.RPCSendTransactionResult{Status: entities.TryAgainLaterStatus}, nil).
					Twice()
			},
			wantErrContains: []string{"transaction did not complete after 2 attempts"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mRPCService := NewRPCServiceMock(t)
			mRPCService.
				On("TrackRPCServiceHealth", mock.Anything).
				Return().
				Once()
			mSignatureClient := signing.NewSignatureClientMock(t)
			mChannelAccountStore := store.NewChannelAccountStoreMock(t)

			if tc.prepareMocks != nil {
				tc.prepareMocks(mRPCService, mSignatureClient, mChannelAccountStore)
			}

			s, err := NewChannelAccountService(ctx, ChannelAccountServiceOptions{
				DB:                                 dbConnectionPool,
				RPCService:                         mRPCService,
				DistributionAccountSignatureClient: mSignatureClient,
				BaseFee:                            100 * txnbuild.MinBaseFee,
				ChannelAccountStore:                mChannelAccountStore,
				PrivateKeyEncrypter:                &signingutils.DefaultPrivateKeyEncrypter{},
				EncryptionPassphrase:               "passphrase",
			})
			require.NoError(t, err)
			time.Sleep(100 * time.Millisecond)

			err = s.submitTransactionWithRetry(ctx, hash, signedTxXDR, retry.Attempts(2), retry.Delay(10*time.Millisecond))
			if tc.wantErrContains != nil {
				require.Error(t, err)
				for _, wantErrContains := range tc.wantErrContains {
					assert.ErrorContains(t, err, wantErrContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_ChannelAccountService_waitForTransactionConfirmation(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	hash := "test_hash"

	testCases := []struct {
		name            string
		prepareMocks    func(mRPCService *RPCServiceMock, mSignatureClient *signing.SignatureClientMock, mChannelAccountStore *store.ChannelAccountStoreMock)
		wantErrContains []string
	}{
		{
			name: "🟢successful_response",
			prepareMocks: func(mRPCService *RPCServiceMock, mSignatureClient *signing.SignatureClientMock, mChannelAccountStore *store.ChannelAccountStoreMock) {
				mRPCService.
					On("GetTransaction", hash).
					Return(entities.RPCGetTransactionResult{Status: entities.SuccessStatus}, nil).
					Once()
			},
		},
		{
			name: "🔴failed_response",
			prepareMocks: func(mRPCService *RPCServiceMock, mSignatureClient *signing.SignatureClientMock, mChannelAccountStore *store.ChannelAccountStoreMock) {
				mRPCService.
					On("GetTransaction", hash).
					Return(entities.RPCGetTransactionResult{
						Status:         entities.FailedStatus,
						ErrorResultXDR: "error_xdr",
					}, nil).
					Once()
			},
			wantErrContains: []string{"transaction with hash", "test_hash", "failed with status FAILED and errorResultXdr error_xdr"},
		},
		{
			name: "🔄🟢successful_response_with_retry",
			prepareMocks: func(mRPCService *RPCServiceMock, mSignatureClient *signing.SignatureClientMock, mChannelAccountStore *store.ChannelAccountStoreMock) {
				mRPCService.
					On("GetTransaction", hash).
					Return(entities.RPCGetTransactionResult{Status: entities.NotFoundStatus}, nil).
					Once().
					On("GetTransaction", hash).
					Return(entities.RPCGetTransactionResult{Status: entities.SuccessStatus}, nil).
					Once()
			},
		},
		{
			name: "🔄🔴failed_response_with_retry",
			prepareMocks: func(mRPCService *RPCServiceMock, mSignatureClient *signing.SignatureClientMock, mChannelAccountStore *store.ChannelAccountStoreMock) {
				mRPCService.
					On("GetTransaction", hash).
					Return(entities.RPCGetTransactionResult{Status: entities.NotFoundStatus}, nil).
					Once().
					On("GetTransaction", hash).
					Return(entities.RPCGetTransactionResult{
						Status:         entities.FailedStatus,
						ErrorResultXDR: "error_xdr",
					}, nil).
					Once()
			},
			wantErrContains: []string{"transaction with hash", "test_hash", "failed with status FAILED and errorResultXdr error_xdr"},
		},
		{
			name: "🔴max_attempts_reached",
			prepareMocks: func(mRPCService *RPCServiceMock, mSignatureClient *signing.SignatureClientMock, mChannelAccountStore *store.ChannelAccountStoreMock) {
				mRPCService.
					On("GetTransaction", hash).
					Return(entities.RPCGetTransactionResult{Status: entities.NotFoundStatus}, nil).
					Twice()
			},
			wantErrContains: []string{"failed to get transaction status after 2 attempts", "transaction not found"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mRPCService := NewRPCServiceMock(t)
			mRPCService.
				On("TrackRPCServiceHealth", mock.Anything).
				Return().
				Once()
			mSignatureClient := signing.NewSignatureClientMock(t)
			mChannelAccountStore := store.NewChannelAccountStoreMock(t)

			if tc.prepareMocks != nil {
				tc.prepareMocks(mRPCService, mSignatureClient, mChannelAccountStore)
			}

			s, err := NewChannelAccountService(ctx, ChannelAccountServiceOptions{
				DB:                                 dbConnectionPool,
				RPCService:                         mRPCService,
				DistributionAccountSignatureClient: mSignatureClient,
				BaseFee:                            100 * txnbuild.MinBaseFee,
				ChannelAccountStore:                mChannelAccountStore,
				PrivateKeyEncrypter:                &signingutils.DefaultPrivateKeyEncrypter{},
				EncryptionPassphrase:               "passphrase",
			})
			require.NoError(t, err)
			time.Sleep(100 * time.Millisecond)

			err = s.waitForTransactionConfirmation(ctx, hash, retry.Attempts(2), retry.Delay(10*time.Millisecond))
			if tc.wantErrContains != nil {
				require.Error(t, err)
				for _, wantErrContains := range tc.wantErrContains {
					assert.ErrorContains(t, err, wantErrContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_ChannelAccountServiceOptions_Validate(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockRPCService := &RPCServiceMock{}
	mockSignatureClient := &signing.SignatureClientMock{}
	mockChannelAccountStore := &store.ChannelAccountStoreMock{}
	mockPrivateKeyEncrypter := &signingutils.DefaultPrivateKeyEncrypter{}

	testCases := []struct {
		name            string
		opts            ChannelAccountServiceOptions
		wantErrContains string
	}{
		{
			name:            "🔴DB_is_required",
			opts:            ChannelAccountServiceOptions{},
			wantErrContains: "DB cannot be nil",
		},
		{
			name: "🔴RPCService_is_required",
			opts: ChannelAccountServiceOptions{
				DB: dbConnectionPool,
			},
			wantErrContains: "rpc client cannot be nil",
		},
		{
			name: "🔴BaseFee_>=_MinBaseFee",
			opts: ChannelAccountServiceOptions{
				DB:         dbConnectionPool,
				RPCService: mockRPCService,
				BaseFee:    txnbuild.MinBaseFee - 1,
			},
			wantErrContains: "base fee is lower than the minimum network fee",
		},
		{
			name: "🔴DistributionAccountSignatureClient_is_required",
			opts: ChannelAccountServiceOptions{
				DB:         dbConnectionPool,
				RPCService: mockRPCService,
				BaseFee:    txnbuild.MinBaseFee,
			},
			wantErrContains: "distribution account signature client cannot be nil",
		},
		{
			name: "🔴ChannelAccountStore_is_required",
			opts: ChannelAccountServiceOptions{
				DB:                                 dbConnectionPool,
				RPCService:                         mockRPCService,
				BaseFee:                            txnbuild.MinBaseFee,
				DistributionAccountSignatureClient: mockSignatureClient,
			},
			wantErrContains: "channel account store cannot be nil",
		},
		{
			name: "🔴PrivateKeyEncrypter_is_required",
			opts: ChannelAccountServiceOptions{
				DB:                                 dbConnectionPool,
				RPCService:                         mockRPCService,
				BaseFee:                            txnbuild.MinBaseFee,
				DistributionAccountSignatureClient: mockSignatureClient,
				ChannelAccountStore:                mockChannelAccountStore,
			},
			wantErrContains: "private key encrypter cannot be nil",
		},
		{
			name: "🔴EncryptionPassphrase_is_required",
			opts: ChannelAccountServiceOptions{
				DB:                                 dbConnectionPool,
				RPCService:                         mockRPCService,
				BaseFee:                            txnbuild.MinBaseFee,
				DistributionAccountSignatureClient: mockSignatureClient,
				ChannelAccountStore:                mockChannelAccountStore,
				PrivateKeyEncrypter:                mockPrivateKeyEncrypter,
			},
			wantErrContains: "encryption passphrase cannot be empty",
		},
		{
			name: "🟢Valid_options",
			opts: ChannelAccountServiceOptions{
				DB:                                 dbConnectionPool,
				RPCService:                         mockRPCService,
				BaseFee:                            txnbuild.MinBaseFee,
				DistributionAccountSignatureClient: mockSignatureClient,
				ChannelAccountStore:                mockChannelAccountStore,
				PrivateKeyEncrypter:                mockPrivateKeyEncrypter,
				EncryptionPassphrase:               "test",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.Validate()
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
