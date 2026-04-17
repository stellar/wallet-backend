package services

import (
	"context"
	"fmt"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
	"github.com/stellar/wallet-backend/internal/utils"
)

func Test_ChannelAccountService_EnsureChannelAccounts(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, outerErr := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, outerErr)
	defer dbConnectionPool.Close()

	type testCase struct {
		name             string
		getCtx           func() context.Context
		numberOfAccounts int64
		setupMocks       func(t *testing.T, mockRPCService *RPCServiceMock, distAccSigClient *signing.SignatureClientMock, chAccSigClient *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock)
		expectedError    string
	}

	testCases := []testCase{
		{
			name:             "🟢sufficient_number_of_channel_accounts",
			numberOfAccounts: 5,
			getCtx:           context.Background,
			setupMocks: func(t *testing.T, _ *RPCServiceMock, _ *signing.SignatureClientMock, _ *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock) {
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
			setupMocks: func(t *testing.T, mockRPCService *RPCServiceMock, distAccSigClient *signing.SignatureClientMock, chAccSigClient *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock) {
				channelAccountStore.
					On("Count", mock.Anything).
					Return(2, nil). // will create 3 channel accounts
					Once()

				distributionAccount := keypair.MustRandom()
				channelAccountsAddressesBeingInserted := []string{}
				signedTx := txnbuild.Transaction{}
				distAccSigClient.
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
					Once()

				// Mock channel account signature client for NetworkPassphrase call
				chAccSigClient.
					On("NetworkPassphrase").
					Return(network.TestNetworkPassphrase).
					Once()

				mockRPCService.
					On("GetHealth").
					Return(entities.RPCGetHealthResult{Status: "healthy"}, nil).
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
					On("BatchInsert", mock.Anything, mock.AnythingOfType("[]*store.ChannelAccount")).
					Run(func(args mock.Arguments) {
						channelAccounts, ok := args.Get(1).([]*store.ChannelAccount)
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
			name:             "🟢successfully_ensures_the_channel_accounts_deletion",
			numberOfAccounts: 5,
			getCtx:           context.Background,
			setupMocks: func(t *testing.T, mockRPCService *RPCServiceMock, distAccSigClient *signing.SignatureClientMock, chAccSigClient *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock) {
				chAcc1 := store.ChannelAccount{PublicKey: keypair.MustRandom().Address()}
				chAcc2 := store.ChannelAccount{PublicKey: keypair.MustRandom().Address()}
				chAcc3 := store.ChannelAccount{PublicKey: keypair.MustRandom().Address()}
				channelAccountStore.
					On("Count", mock.Anything).
					Return(8, nil). // will delete 3 channel accounts
					Once().
					On("GetAllForUpdate", mock.Anything, mock.Anything, 3).
					Return([]*store.ChannelAccount{&chAcc1, &chAcc2, &chAcc3}, nil).
					Once()

				distributionAccount := keypair.MustRandom()
				channelAccountsAddressesBeingDeleted := []string{}
				signedTx := txnbuild.Transaction{}
				distAccSigClient.
					On("GetAccountPublicKey", mock.Anything).
					Return(distributionAccount.Address(), nil).
					Once().
					On("SignStellarTransaction", mock.Anything, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
					Run(func(args mock.Arguments) {
						tx, ok := args.Get(1).(*txnbuild.Transaction)
						require.True(t, ok)

						assert.Equal(t, distributionAccount.Address(), tx.SourceAccount().AccountID)
						assert.Len(t, tx.Operations(), 3)

						for _, op := range tx.Operations() {
							mergeOp, ok := op.(*txnbuild.AccountMerge)
							require.True(t, ok)
							assert.Equal(t, distributionAccount.Address(), mergeOp.Destination)
							channelAccountsAddressesBeingDeleted = append(channelAccountsAddressesBeingDeleted, mergeOp.SourceAccount)
						}

						tx, err := tx.Sign(network.TestNetworkPassphrase, distributionAccount)
						require.NoError(t, err)

						signedTx = *tx
					}).
					Return(&signedTx, nil).
					Once().
					On("NetworkPassphrase").
					Return(network.TestNetworkPassphrase).
					Once()

				// Mock channel account signature client for NetworkPassphrase call
				chAccSigClient.
					On("SignStellarTransaction", mock.Anything, mock.AnythingOfType("*txnbuild.Transaction"), []string{chAcc1.PublicKey, chAcc2.PublicKey, chAcc3.PublicKey}).
					Return(&signedTx, nil).
					Once()

				mockRPCService.
					On("GetHealth").Return(entities.RPCGetHealthResult{Status: "healthy"}, nil).Once().
					On("GetAccountLedgerSequence", distributionAccount.Address()).Return(int64(123), nil).Once().
					On("GetAccountLedgerSequence", chAcc1.PublicKey).Return(int64(123), nil).Once().
					On("GetAccountLedgerSequence", chAcc2.PublicKey).Return(int64(123), nil).Once().
					On("GetAccountLedgerSequence", chAcc3.PublicKey).Return(int64(123), nil).Once().
					On("SendTransaction", mock.AnythingOfType("string")).Return(entities.RPCSendTransactionResult{Status: entities.PendingStatus}, nil).Once().
					On("GetTransaction", mock.AnythingOfType("string")).Return(entities.RPCGetTransactionResult{Status: entities.SuccessStatus}, nil).Once()

				channelAccountStore.
					On("Delete", mock.Anything, mock.Anything, mock.AnythingOfType("[]string")).
					Run(func(args mock.Arguments) {
						channelAccountsAddresses, ok := args.Get(2).([]string)
						require.True(t, ok)

						assert.ElementsMatch(t, channelAccountsAddressesBeingDeleted, channelAccountsAddresses)
					}).
					Return(int64(3), nil).
					Once()
			},
		},
		{
			name:             "🔴fails_when_transaction_submission_fails",
			numberOfAccounts: 5,
			getCtx:           context.Background,
			setupMocks: func(t *testing.T, mockRPCService *RPCServiceMock, distAccSigClient *signing.SignatureClientMock, chAccSigClient *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock) {
				channelAccountStore.
					On("Count", mock.Anything).
					Return(2, nil).
					Once()

				distributionAccount := keypair.MustRandom()
				signedTx := txnbuild.Transaction{}
				distAccSigClient.
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
					Once()

				// Mock channel account signature client for NetworkPassphrase call
				chAccSigClient.
					On("NetworkPassphrase").
					Return(network.TestNetworkPassphrase).
					Once()

				mockRPCService.
					On("GetHealth").
					Return(entities.RPCGetHealthResult{Status: "healthy"}, nil).
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
			setupMocks: func(t *testing.T, mockRPCService *RPCServiceMock, distAccSigClient *signing.SignatureClientMock, chAccSigClient *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock) {
				channelAccountStore.
					On("Count", mock.Anything).
					Return(2, nil).
					Once()

				distributionAccount := keypair.MustRandom()
				signedTx := txnbuild.Transaction{}
				distAccSigClient.
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
					Once()

				// Mock channel account signature client for NetworkPassphrase call
				chAccSigClient.
					On("NetworkPassphrase").
					Return(network.TestNetworkPassphrase).
					Once()

				mockRPCService.
					On("GetHealth").
					Return(entities.RPCGetHealthResult{Status: "healthy"}, nil).
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
			name:             "🔴fails_if_rpc_service_is_not_healthy",
			numberOfAccounts: 5,
			getCtx:           func() context.Context { ctx, cancel := context.WithCancel(context.Background()); cancel(); return ctx },
			setupMocks: func(t *testing.T, mockRPCService *RPCServiceMock, distAccSigClient *signing.SignatureClientMock, chAccSigClient *signing.SignatureClientMock, channelAccountStore *store.ChannelAccountStoreMock) {
				channelAccountStore.
					On("Count", mock.Anything).
					Return(2, nil).
					Once()

				distributionAccount := keypair.MustRandom()
				distAccSigClient.
					On("GetAccountPublicKey", mock.Anything).
					Return(distributionAccount.Address(), nil).
					Once()

				mockRPCService.
					On("GetHealth").
					Return(entities.RPCGetHealthResult{}, fmt.Errorf("RPC unavailable")).
					Maybe()
			},
			expectedError: "timeout waiting for RPC service to become healthy",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create fresh mocks for each test
			ctx := tc.getCtx()
			mockRPCService := NewRPCServiceMock(t)
			distAccSigClient := signing.NewSignatureClientMock(t)
			chAccSigClient := signing.NewSignatureClientMock(t)
			channelAccountStore := store.NewChannelAccountStoreMock(t)

			// Setup mocks
			tc.setupMocks(t, mockRPCService, distAccSigClient, chAccSigClient, channelAccountStore)

			// Create service with fresh mocks
			s, err := NewChannelAccountService(ctx, ChannelAccountServiceOptions{
				DB:                                 dbConnectionPool,
				RPCService:                         mockRPCService,
				BaseFee:                            txnbuild.MinBaseFee,
				DistributionAccountSignatureClient: distAccSigClient,
				ChannelAccountSignatureClient:      chAccSigClient,
				ChannelAccountStore:                channelAccountStore,
				PrivateKeyEncrypter:                &signingutils.DefaultPrivateKeyEncrypter{},
				EncryptionPassphrase:               "my-encryption-passphrase",
			})
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

func Test_ChannelAccountService_ValidateChannelAccounts(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	type testCase struct {
		name          string
		minimum       int64
		setupMocks    func(channelAccountStore *store.ChannelAccountStoreMock, rpcService *RPCServiceMock)
		expectedError string
	}

	channelAccount1 := &store.ChannelAccount{PublicKey: keypair.MustRandom().Address()}
	channelAccount2 := &store.ChannelAccount{PublicKey: keypair.MustRandom().Address()}
	channelAccount3 := &store.ChannelAccount{PublicKey: keypair.MustRandom().Address()}

	testCases := []testCase{
		{
			name:    "list all fails",
			minimum: 2,
			setupMocks: func(channelAccountStore *store.ChannelAccountStoreMock, _ *RPCServiceMock) {
				channelAccountStore.On("GetAll", mock.Anything).Return(nil, fmt.Errorf("read failed")).Once()
			},
			expectedError: "listing stored channel accounts: read failed",
		},
		{
			name:    "stored count below minimum",
			minimum: 2,
			setupMocks: func(channelAccountStore *store.ChannelAccountStoreMock, _ *RPCServiceMock) {
				channelAccountStore.On("GetAll", mock.Anything).Return([]*store.ChannelAccount{channelAccount1}, nil).Once()
			},
			expectedError: "stored channel account count 1 is below required minimum 2",
		},
		{
			name:    "stored account missing on network",
			minimum: 1,
			setupMocks: func(channelAccountStore *store.ChannelAccountStoreMock, rpcService *RPCServiceMock) {
				channelAccountStore.On("GetAll", mock.Anything).Return([]*store.ChannelAccount{channelAccount1}, nil).Once()
				rpcService.On("GetLedgerEntries", mock.AnythingOfType("[]string")).Return(entities.RPCGetLedgerEntriesResult{}, nil).Once()
			},
			expectedError: fmt.Sprintf("channel account %s does not exist on the configured Stellar network", channelAccount1.PublicKey),
		},
		{
			name:    "rpc failure during validation",
			minimum: 1,
			setupMocks: func(channelAccountStore *store.ChannelAccountStoreMock, rpcService *RPCServiceMock) {
				channelAccountStore.On("GetAll", mock.Anything).Return([]*store.ChannelAccount{channelAccount1}, nil).Once()
				rpcService.On("GetLedgerEntries", mock.AnythingOfType("[]string")).Return(entities.RPCGetLedgerEntriesResult{}, fmt.Errorf("rpc unavailable")).Once()
			},
			expectedError: "getting ledger entries for channel accounts: rpc unavailable",
		},
		{
			name:    "invalid returned xdr",
			minimum: 1,
			setupMocks: func(channelAccountStore *store.ChannelAccountStoreMock, rpcService *RPCServiceMock) {
				channelAccountStore.On("GetAll", mock.Anything).Return([]*store.ChannelAccount{channelAccount1}, nil).Once()
				rpcService.On("GetLedgerEntries", mock.AnythingOfType("[]string")).Return(entities.RPCGetLedgerEntriesResult{
					Entries: []entities.LedgerEntryResult{
						makeRPCAccountLedgerEntryResultWithData(t, channelAccount1.PublicKey, "not-base64"),
					},
				}, nil).Once()
			},
			expectedError: fmt.Sprintf("decoding account entry for channel account %s", channelAccount1.PublicKey),
		},
		{
			name:    "wrong returned ledger entry type",
			minimum: 1,
			setupMocks: func(channelAccountStore *store.ChannelAccountStoreMock, rpcService *RPCServiceMock) {
				channelAccountStore.On("GetAll", mock.Anything).Return([]*store.ChannelAccount{channelAccount1}, nil).Once()
				rpcService.On("GetLedgerEntries", mock.AnythingOfType("[]string")).Return(entities.RPCGetLedgerEntriesResult{
					Entries: []entities.LedgerEntryResult{
						makeRPCTrustlineLedgerEntryResult(t, channelAccount1.PublicKey),
					},
				}, nil).Once()
			},
			expectedError: fmt.Sprintf("channel account %s returned non-account ledger entry type", channelAccount1.PublicKey),
		},
		{
			name:    "success with exact configured count",
			minimum: 2,
			setupMocks: func(channelAccountStore *store.ChannelAccountStoreMock, rpcService *RPCServiceMock) {
				channelAccountStore.On("GetAll", mock.Anything).Return([]*store.ChannelAccount{channelAccount1, channelAccount2}, nil).Once()
				rpcService.On("GetLedgerEntries", mock.AnythingOfType("[]string")).Return(entities.RPCGetLedgerEntriesResult{
					Entries: []entities.LedgerEntryResult{
						makeRPCAccountLedgerEntryResult(t, channelAccount1.PublicKey),
						makeRPCAccountLedgerEntryResult(t, channelAccount2.PublicKey),
					},
				}, nil).Once()
			},
		},
		{
			name:    "success with extra valid accounts",
			minimum: 1,
			setupMocks: func(channelAccountStore *store.ChannelAccountStoreMock, rpcService *RPCServiceMock) {
				channelAccountStore.On("GetAll", mock.Anything).Return([]*store.ChannelAccount{channelAccount1, channelAccount2}, nil).Once()
				rpcService.On("GetLedgerEntries", mock.AnythingOfType("[]string")).Return(entities.RPCGetLedgerEntriesResult{
					Entries: []entities.LedgerEntryResult{
						makeRPCAccountLedgerEntryResult(t, channelAccount1.PublicKey),
						makeRPCAccountLedgerEntryResult(t, channelAccount2.PublicKey),
					},
				}, nil).Once()
			},
		},
		{
			name:    "missing account is reported in stored order",
			minimum: 3,
			setupMocks: func(channelAccountStore *store.ChannelAccountStoreMock, rpcService *RPCServiceMock) {
				channelAccountStore.On("GetAll", mock.Anything).Return([]*store.ChannelAccount{channelAccount1, channelAccount2, channelAccount3}, nil).Once()
				rpcService.On("GetLedgerEntries", mock.AnythingOfType("[]string")).Return(entities.RPCGetLedgerEntriesResult{
					Entries: []entities.LedgerEntryResult{
						makeRPCAccountLedgerEntryResult(t, channelAccount2.PublicKey),
						makeRPCAccountLedgerEntryResult(t, channelAccount3.PublicKey),
					},
				}, nil).Once()
			},
			expectedError: fmt.Sprintf("channel account %s does not exist on the configured Stellar network", channelAccount1.PublicKey),
		},
		{
			name:    "batches getLedgerEntries requests",
			minimum: 201,
			setupMocks: func(channelAccountStore *store.ChannelAccountStoreMock, rpcService *RPCServiceMock) {
				channelAccounts := make([]*store.ChannelAccount, 0, 201)
				ledgerKeys := make([]string, 0, 201)
				entries := make([]entities.LedgerEntryResult, 0, 201)
				for range 201 {
					channelAccount := &store.ChannelAccount{PublicKey: keypair.MustRandom().Address()}
					channelAccounts = append(channelAccounts, channelAccount)
					ledgerKey, err := utils.GetAccountLedgerKey(channelAccount.PublicKey)
					require.NoError(t, err)
					ledgerKeys = append(ledgerKeys, ledgerKey)
					entries = append(entries, makeRPCAccountLedgerEntryResult(t, channelAccount.PublicKey))
				}

				channelAccountStore.On("GetAll", mock.Anything).Return(channelAccounts, nil).Once()
				rpcService.On("GetLedgerEntries", ledgerKeys[:200]).Return(entities.RPCGetLedgerEntriesResult{
					Entries: entries[:200],
				}, nil).Once()
				rpcService.On("GetLedgerEntries", ledgerKeys[200:]).Return(entities.RPCGetLedgerEntriesResult{
					Entries: entries[200:],
				}, nil).Once()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rpcService := NewRPCServiceMock(t)
			channelAccountStore := store.NewChannelAccountStoreMock(t)
			service, err := NewChannelAccountService(ctx, ChannelAccountServiceOptions{
				DB:                                 dbConnectionPool,
				RPCService:                         rpcService,
				BaseFee:                            txnbuild.MinBaseFee,
				DistributionAccountSignatureClient: signing.NewSignatureClientMock(t),
				ChannelAccountSignatureClient:      signing.NewSignatureClientMock(t),
				ChannelAccountStore:                channelAccountStore,
				PrivateKeyEncrypter:                &signingutils.DefaultPrivateKeyEncrypter{},
				EncryptionPassphrase:               "my-encryption-passphrase",
			})
			require.NoError(t, err)

			tc.setupMocks(channelAccountStore, rpcService)

			err = service.ValidateChannelAccounts(ctx, tc.minimum)

			if tc.expectedError != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func makeRPCAccountLedgerEntryResult(t *testing.T, address string) entities.LedgerEntryResult {
	t.Helper()

	aid := xdr.MustAddress(address)
	dataXDR, err := xdr.MarshalBase64(xdr.LedgerEntryData{
		Type: xdr.LedgerEntryTypeAccount,
		Account: &xdr.AccountEntry{
			AccountId:  aid,
			Balance:    1000000000,
			SeqNum:     xdr.SequenceNumber(1),
			Thresholds: xdr.NewThreshold(1, 1, 1, 1),
			Flags:      0,
			Ext:        xdr.AccountEntryExt{V: 0},
		},
	})
	require.NoError(t, err)

	return makeRPCAccountLedgerEntryResultWithData(t, address, dataXDR)
}

func makeRPCTrustlineLedgerEntryResult(t *testing.T, address string) entities.LedgerEntryResult {
	t.Helper()

	aid := xdr.MustAddress(address)
	dataXDR, err := xdr.MarshalBase64(xdr.LedgerEntryData{
		Type: xdr.LedgerEntryTypeTrustline,
		TrustLine: &xdr.TrustLineEntry{
			AccountId: aid,
			Asset:     xdr.MustNewCreditAsset("TEST", keypair.MustRandom().Address()).ToTrustLineAsset(),
			Balance:   xdr.Int64(100),
			Limit:     xdr.Int64(1000),
			Flags:     0,
			Ext:       xdr.TrustLineEntryExt{V: 0},
		},
	})
	require.NoError(t, err)

	return makeRPCAccountLedgerEntryResultWithData(t, address, dataXDR)
}

func makeRPCAccountLedgerEntryResultWithData(t *testing.T, address, dataXDR string) entities.LedgerEntryResult {
	t.Helper()

	keyXDR, err := utils.GetAccountLedgerKey(address)
	require.NoError(t, err)

	return entities.LedgerEntryResult{
		KeyXDR:  keyXDR,
		DataXDR: dataXDR,
	}
}

func TestSubmitTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	mockRPCService := RPCServiceMock{}
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
	require.NoError(t, err)

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

	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	mockRPCService := RPCServiceMock{}
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
	require.NoError(t, err)

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
