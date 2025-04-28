package services

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
)

func buildInvokeContractOp(t *testing.T) *txnbuild.InvokeHostFunction {
	t.Helper()

	var nativeAssetContractID xdr.Hash
	var err error
	nativeAssetContractID, err = xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(network.TestNetworkPassphrase)
	require.NoError(t, err)

	accountID, err := xdr.AddressToAccountId("GDPQASWWPBLHZBAJVTOXYQKM57LRIMXVA6OHMVUVRLYQB7PRE4FYVFEG")
	require.NoError(t, err)
	scAddress := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &accountID,
	}

	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &nativeAssetContractID,
				},
				FunctionName: "transfer",
				Args: xdr.ScVec{
					{
						Type:    xdr.ScValTypeScvAddress,
						Address: &scAddress,
					},
					{
						Type:    xdr.ScValTypeScvAddress,
						Address: &scAddress,
					},
					{
						Type: xdr.ScValTypeScvI128,
						I128: &xdr.Int128Parts{
							Hi: xdr.Int64(0),
							Lo: xdr.Uint64(uint64(amount.MustParse("10"))),
						},
					},
				},
			},
		},
		SourceAccount: "",
	}
}

func buildPaymentOp(t *testing.T) *txnbuild.Payment {
	t.Helper()

	return &txnbuild.Payment{
		Destination:   "GBJLFRJO2V7QYHJGJ4WAOCKWNH3WKG5XRQMTPXIZQSC3QW6MA5JRHMJ7",
		Amount:        "10.0000000",
		Asset:         txnbuild.NativeAsset{},
		SourceAccount: "GCZUXU4NRAY62KKGQAQ7DSRZTNWGOWYLTCWU4QICNRTHLIQIDQHPTFBH",
	}
}

func TestValidateOptions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	t.Run("return_error_when_db_nil", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: nil,
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			ChannelAccountStore:                &store.ChannelAccountStoreMock{},
			RPCService:                         &services.RPCServiceMock{},
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "DB cannot be nil", err.Error())
	})
	t.Run("return_error_when_distribution_signature_client_nil", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DB:                                 dbConnectionPool,
			DistributionAccountSignatureClient: nil,
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			ChannelAccountStore:                &store.ChannelAccountStoreMock{},
			RPCService:                         &services.RPCServiceMock{},
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "distribution account signature client cannot be nil", err.Error())
	})

	t.Run("return_error_when_channel_signature_client_nil", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DB:                                 dbConnectionPool,
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      nil,
			ChannelAccountStore:                &store.ChannelAccountStoreMock{},
			RPCService:                         &services.RPCServiceMock{},
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "channel account signature client cannot be nil", err.Error())
	})

	t.Run("return_error_when_channel_account_store_nil", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DB:                                 dbConnectionPool,
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			ChannelAccountStore:                nil,
			RPCService:                         &services.RPCServiceMock{},
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "channel account store cannot be nil", err.Error())
	})

	t.Run("return_error_when_rpc_client_nil", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DB:                                 dbConnectionPool,
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			ChannelAccountStore:                &store.ChannelAccountStoreMock{},
			RPCService:                         nil,
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "rpc client cannot be nil", err.Error())
	})

	t.Run("return_error_when_base_fee_too_low", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DB:                                 dbConnectionPool,
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			ChannelAccountStore:                &store.ChannelAccountStoreMock{},
			RPCService:                         &services.RPCServiceMock{},
			BaseFee:                            txnbuild.MinBaseFee - 10,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "base fee is lower than the minimum network fee", err.Error())
	})
}

func TestBuildAndSignTransactionWithChannelAccount(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mDistributionAccountSignatureClient := signing.SignatureClientMock{}
	mChannelAccountSignatureClient := signing.SignatureClientMock{}
	mChannelAccountStore := store.ChannelAccountStoreMock{}
	mRPCService := services.RPCServiceMock{}
	txService, err := NewTransactionService(TransactionServiceOptions{
		DB:                                 dbConnectionPool,
		DistributionAccountSignatureClient: &mDistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      &mChannelAccountSignatureClient,
		ChannelAccountStore:                &mChannelAccountStore,
		RPCService:                         &mRPCService,
		BaseFee:                            114,
	})
	require.NoError(t, err)

	t.Run("🔴timeout_must_be_smaller_than_max_timeout", func(t *testing.T) {
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, MaxTimeoutInSeconds+1)
		assert.Empty(t, tx)
		assert.EqualError(t, err, fmt.Sprintf("cannot be greater than %d seconds", MaxTimeoutInSeconds))
	})

	t.Run("🔴handle_GetAccountPublicKey_err", func(t *testing.T) {
		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return("", errors.New("channel accounts unavailable")).
			Once()

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, 30)

		mChannelAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.EqualError(t, err, "getting channel account public key: channel accounts unavailable")
	})

	t.Run("🚨operation_source_account_cannot_be_channel_account", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return(channelAccount.Address(), nil).
			Once()

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{&txnbuild.AccountMerge{
			Destination:   keypair.MustRandom().Address(),
			SourceAccount: channelAccount.Address(),
		}}, 30)

		mChannelAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.ErrorContains(t, err, "operation source account cannot be the channel account public key")
	})

	t.Run("🚨operation_source_account_cannot_be_empty", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return(channelAccount.Address(), nil).
			Once()

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{&txnbuild.AccountMerge{
			Destination: keypair.MustRandom().Address(),
		}}, 30)

		mChannelAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.ErrorContains(t, err, "operation source account cannot be empty")
	})

	t.Run("🔴handle_GetAccountLedgerSequence_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return(channelAccount.Address(), nil).
			Once()

		mRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(0), errors.New("rpc service down")).
			Once()
		defer mRPCService.AssertExpectations(t)

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, 30)

		mChannelAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, tx)
		expectedErr := fmt.Errorf("getting ledger sequence for channel account public key %q: rpc service down", channelAccount.Address())
		assert.EqualError(t, err, expectedErr.Error())
	})

	t.Run("🔴handle_NewTransaction_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return(channelAccount.Address(), nil).
			Once()

		mRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mRPCService.AssertExpectations(t)

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, 30)

		mChannelAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.EqualError(t, err, "building transaction: transaction has no operations")
	})

	t.Run("🔴handle_AssignTxToChannelAccount_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return(channelAccount.Address(), nil).
			Once().
			On("NetworkPassphrase").
			Return("networkpassphrase").
			Once()

		mChannelAccountStore.
			On("AssignTxToChannelAccount", context.Background(), channelAccount.Address(), mock.AnythingOfType("string")).
			Return(errors.New("unable to assign channel account to tx")).
			Once()

		mRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mRPCService.AssertExpectations(t)

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{buildPaymentOp(t)}, 30)

		mChannelAccountSignatureClient.AssertExpectations(t)
		mChannelAccountStore.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.EqualError(t, err, "assigning channel account to tx: unable to assign channel account to tx")
	})

	t.Run("🔴handle_SignStellarTransaction_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return(channelAccount.Address(), nil).
			Once().
			On("NetworkPassphrase").
			Return("networkpassphrase").
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{channelAccount.Address()}).
			Return(nil, errors.New("unable to sign")).
			Once()

		mChannelAccountStore.
			On("AssignTxToChannelAccount", context.Background(), channelAccount.Address(), mock.AnythingOfType("string")).
			Return(nil).
			Once()

		mRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mRPCService.AssertExpectations(t)

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{buildPaymentOp(t)}, 30)

		mChannelAccountSignatureClient.AssertExpectations(t)
		mChannelAccountStore.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.EqualError(t, err, "signing transaction with channel account: unable to sign")
	})

	t.Run("🟢build_and_sign_tx_with_channel_account", func(t *testing.T) {
		signedTx := utils.BuildTestTransaction(t)
		channelAccount := keypair.MustRandom()

		successTestCases := []struct {
			name              string
			inputTimeout      int
			consideredTimeout int
		}{
			{
				name:              "timeout_is_negative",
				inputTimeout:      -1,
				consideredTimeout: DefaultTimeoutInSeconds,
			},
			{
				name:              "timeout_is_zero",
				inputTimeout:      0,
				consideredTimeout: DefaultTimeoutInSeconds,
			},
			{
				name:              "timeout_is_positive",
				inputTimeout:      10,
				consideredTimeout: 10,
			},
			{
				name:              "timeout_is_max",
				inputTimeout:      MaxTimeoutInSeconds,
				consideredTimeout: MaxTimeoutInSeconds,
			},
		}
		for _, tc := range successTestCases {
			t.Run(tc.name, func(t *testing.T) {
				mChannelAccountSignatureClient.
					On("GetAccountPublicKey", context.Background(), tc.consideredTimeout).
					Return(channelAccount.Address(), nil).
					Once().
					On("NetworkPassphrase").
					Return("networkpassphrase").
					On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{channelAccount.Address()}).
					Return(signedTx, nil).
					Once()

				mChannelAccountStore.
					On("AssignTxToChannelAccount", context.Background(), channelAccount.Address(), mock.AnythingOfType("string")).
					Return(nil).
					Once()

				mRPCService.
					On("GetAccountLedgerSequence", channelAccount.Address()).
					Return(int64(1), nil).
					Once()
				defer mRPCService.AssertExpectations(t)

				tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{buildPaymentOp(t)}, int64(tc.inputTimeout))

				mChannelAccountSignatureClient.AssertExpectations(t)
				mChannelAccountStore.AssertExpectations(t)
				assert.Equal(t, signedTx, tx)
				assert.NoError(t, err)
			})
		}
	})
}

func TestBuildFeeBumpTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	distributionAccountSignatureClient := signing.SignatureClientMock{}
	channelAccountSignatureClient := signing.SignatureClientMock{}
	channelAccountStore := store.ChannelAccountStoreMock{}
	mockRPCService := &services.RPCServiceMock{}
	txService, err := NewTransactionService(TransactionServiceOptions{
		DB:                                 dbConnectionPool,
		DistributionAccountSignatureClient: &distributionAccountSignatureClient,
		ChannelAccountSignatureClient:      &channelAccountSignatureClient,
		ChannelAccountStore:                &channelAccountStore,
		RPCService:                         mockRPCService,
		BaseFee:                            114,
	})
	require.NoError(t, err)
	t.Run("distribution_account_signature_client_get_account_public_key_err", func(t *testing.T) {
		tx := utils.BuildTestTransaction(t)
		distributionAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return("", errors.New("channel accounts unavailable")).
			Once()

		feeBumpTx, err := txService.BuildFeeBumpTransaction(context.Background(), tx)

		distributionAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "getting distribution account public key: channel accounts unavailable", err.Error())
	})

	t.Run("building_tx_fails", func(t *testing.T) {
		distributionAccount := keypair.MustRandom()
		distributionAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(distributionAccount.Address(), nil).
			Once()

		feeBumpTx, err := txService.BuildFeeBumpTransaction(context.Background(), nil)

		distributionAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "building fee-bump transaction inner transaction is missing", err.Error())
	})

	t.Run("signing_feebump_tx_fails", func(t *testing.T) {
		tx := utils.BuildTestTransaction(t)
		distributionAccount := keypair.MustRandom()
		distributionAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(distributionAccount.Address(), nil).
			Once().
			On("SignStellarFeeBumpTransaction", context.Background(), mock.AnythingOfType("*txnbuild.FeeBumpTransaction")).
			Return(nil, errors.New("unable to sign fee bump transaction")).
			Once()

		feeBumpTx, err := txService.BuildFeeBumpTransaction(context.Background(), tx)

		distributionAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "signing the fee bump transaction with distribution account: unable to sign fee bump transaction", err.Error())
	})

	t.Run("returns_singed_feebump_tx", func(t *testing.T) {
		tx := utils.BuildTestTransaction(t)
		feeBump := utils.BuildTestFeeBumpTransaction(t)
		distributionAccount := keypair.MustRandom()
		distributionAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(distributionAccount.Address(), nil).
			Once().
			On("SignStellarFeeBumpTransaction", context.Background(), mock.AnythingOfType("*txnbuild.FeeBumpTransaction")).
			Return(feeBump, nil).
			Once()

		feeBumpTx, err := txService.BuildFeeBumpTransaction(context.Background(), tx)

		distributionAccountSignatureClient.AssertExpectations(t)
		assert.Equal(t, feeBump, feeBumpTx)
		assert.NoError(t, err)
	})
}

func Test_transactionService_prepareForSorobanTransaction(t *testing.T) {
	const chAccPublicKey = "GAB3B3MFQHSEN4YOTZBKKEL7VTWJQ6SWOWAE36I7CWZP3I3A3VT464KG"
	txSourceAccount := keypair.MustRandom().Address()

	sorobanTxDataXDR := "AAAAAAAAAAEAAAAGAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAAFAAAAAEAAAACAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAYAAAAAAAAAABBua0YUOtfPBN8bgJF1VXvNrYCFtsmcg8h+N5Pf2BylAAAAFWhSXFSqNynLAAAAAAAKehUAAAGIAAAA3AAAAAAAAgi1"
	var sorobanTxData xdr.SorobanTransactionData
	err := xdr.SafeUnmarshalBase64(sorobanTxDataXDR, &sorobanTxData)
	require.NoError(t, err)
	require.Equal(t, xdr.Int64(133301), sorobanTxData.ResourceFee)

	testCases := []struct {
		name                string
		baseFee             int64
		incomingOps         []txnbuild.Operation
		prepareMocksFn      func(t *testing.T, mRPCService *services.RPCServiceMock)
		wantBuildTxParamsFn func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams
		wantErrContains     string
	}{
		{
			name:    "🟢no_op_if_ops_are_not_soroban",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildPaymentOp(t),
			},
			wantBuildTxParamsFn: func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams {
				return initialBuildTxParams
			},
		},
		{
			name:    "🔴multiple_ops_where_one_is_soroban",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildPaymentOp(t),
				buildInvokeContractOp(t),
			},
			wantErrContains: "when soroban is used only one operation is allowed",
		},
		{
			name:    "🔴multiple_ops_where_all_are_soroban",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
				buildInvokeContractOp(t),
			},
			wantErrContains: "when soroban is used only one operation is allowed",
		},
		{
			name:    "🔴handle_simulateTransaction_err",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
			},
			prepareMocksFn: func(t *testing.T, mRPCService *services.RPCServiceMock) {
				mRPCService.
					On("SimulateTransaction", mock.AnythingOfType("string"), mock.AnythingOfType("entities.RPCResourceConfig")).
					Return(entities.RPCSimulateTransactionResult{}, errors.New("simulate transaction failed")).
					Once()
			},
			wantErrContains: "simulating transaction: simulate transaction failed",
		},
		{
			name:    "🔴handle_simulateTransaction_error_in_payload",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
			},
			prepareMocksFn: func(t *testing.T, mRPCService *services.RPCServiceMock) {
				simulationResponse := entities.RPCSimulateTransactionResult{
					Error: "simulate transaction failed because fooBar",
				}
				mRPCService.
					On("SimulateTransaction", mock.AnythingOfType("string"), mock.AnythingOfType("entities.RPCResourceConfig")).
					Return(simulationResponse, nil).
					Once()
			},
			wantErrContains: "transaction simulation failed with error=simulate transaction failed because fooBar",
		},
		{
			name:    "🚨catch_txSource=channelAccount",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
			},
			prepareMocksFn: func(t *testing.T, mRPCService *services.RPCServiceMock) {
				channelAccountID, err := xdr.AddressToAccountId(chAccPublicKey)
				require.NoError(t, err)

				simulationResponse := entities.RPCSimulateTransactionResult{
					Results: []entities.RPCSimulateHostFunctionResult{
						{
							Auth: []xdr.SorobanAuthorizationEntry{
								{
									Credentials: xdr.SorobanCredentials{
										Type: xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount,
										Address: &xdr.SorobanAddressCredentials{
											Address: xdr.ScAddress{
												Type:      xdr.ScAddressTypeScAddressTypeAccount,
												AccountId: &channelAccountID,
											},
										},
									},
								},
							},
						},
					},
				}

				mRPCService.
					On("SimulateTransaction", mock.AnythingOfType("string"), mock.AnythingOfType("entities.RPCResourceConfig")).
					Return(simulationResponse, nil).
					Once()
			},
			wantErrContains: "operation source account cannot be the channel account public key",
		},
		{
			name:    "🟢successful_with_larger_base_fee",
			baseFee: 100 * txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
			},
			prepareMocksFn: func(t *testing.T, mRPCService *services.RPCServiceMock) {
				authEntryAccountID, err := xdr.AddressToAccountId(keypair.MustRandom().Address())
				require.NoError(t, err)

				simulationResponse := entities.RPCSimulateTransactionResult{
					TransactionData: sorobanTxData,
					Results: []entities.RPCSimulateHostFunctionResult{
						{
							Auth: []xdr.SorobanAuthorizationEntry{
								{
									Credentials: xdr.SorobanCredentials{
										Type: xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount,
										Address: &xdr.SorobanAddressCredentials{
											Address: xdr.ScAddress{
												Type:      xdr.ScAddressTypeScAddressTypeAccount,
												AccountId: &authEntryAccountID,
											},
										},
									},
								},
							},
						},
					},
				}

				mRPCService.
					On("SimulateTransaction", mock.AnythingOfType("string"), mock.AnythingOfType("entities.RPCResourceConfig")).
					Return(simulationResponse, nil).
					Once()
			},
			wantBuildTxParamsFn: func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams {
				require.Empty(t, buildInvokeContractOp(t).Ext)
				newInvokeContractOp := buildInvokeContractOp(t)
				newInvokeContractOp.Ext, err = xdr.NewTransactionExt(1, sorobanTxData)
				require.NoError(t, err)

				return txnbuild.TransactionParams{
					Operations: []txnbuild.Operation{newInvokeContractOp},
					BaseFee:    initialBuildTxParams.BaseFee / 2,
					SourceAccount: &txnbuild.SimpleAccount{
						AccountID: txSourceAccount,
						Sequence:  1,
					},
					Preconditions: txnbuild.Preconditions{
						TimeBounds: txnbuild.NewTimeout(300),
					},
				}
			},
		},
		{
			name:    "🟢successful_with_min_base_fee",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
			},
			prepareMocksFn: func(t *testing.T, mRPCService *services.RPCServiceMock) {
				authEntryAccountID, err := xdr.AddressToAccountId(keypair.MustRandom().Address())
				require.NoError(t, err)

				simulationResponse := entities.RPCSimulateTransactionResult{
					TransactionData: sorobanTxData,
					Results: []entities.RPCSimulateHostFunctionResult{
						{
							Auth: []xdr.SorobanAuthorizationEntry{
								{
									Credentials: xdr.SorobanCredentials{
										Type: xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount,
										Address: &xdr.SorobanAddressCredentials{
											Address: xdr.ScAddress{
												Type:      xdr.ScAddressTypeScAddressTypeAccount,
												AccountId: &authEntryAccountID,
											},
										},
									},
								},
							},
						},
					},
				}

				mRPCService.
					On("SimulateTransaction", mock.AnythingOfType("string"), mock.AnythingOfType("entities.RPCResourceConfig")).
					Return(simulationResponse, nil).
					Once()
			},
			wantBuildTxParamsFn: func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams {
				require.Empty(t, buildInvokeContractOp(t).Ext)
				newInvokeContractOp := buildInvokeContractOp(t)
				newInvokeContractOp.Ext, err = xdr.NewTransactionExt(1, sorobanTxData)
				require.NoError(t, err)

				return txnbuild.TransactionParams{
					Operations: []txnbuild.Operation{newInvokeContractOp},
					BaseFee:    initialBuildTxParams.BaseFee,
					SourceAccount: &txnbuild.SimpleAccount{
						AccountID: txSourceAccount,
						Sequence:  1,
					},
					Preconditions: txnbuild.Preconditions{
						TimeBounds: txnbuild.NewTimeout(300),
					},
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mRPCService := &services.RPCServiceMock{}
			if tc.prepareMocksFn != nil {
				tc.prepareMocksFn(t, mRPCService)
			}
			txService := &transactionService{
				RPCService: mRPCService,
				BaseFee:    tc.baseFee,
			}

			incomingBuildTxParams := txnbuild.TransactionParams{
				Operations: tc.incomingOps,
				BaseFee:    tc.baseFee,
				SourceAccount: &txnbuild.SimpleAccount{
					AccountID: txSourceAccount,
					Sequence:  1,
				},
				Preconditions: txnbuild.Preconditions{
					TimeBounds: txnbuild.NewTimeout(300),
				},
			}

			buildTxParams, err := txService.prepareForSorobanTransaction(context.Background(), chAccPublicKey, incomingBuildTxParams)
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				assert.Empty(t, buildTxParams)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.wantBuildTxParamsFn(t, incomingBuildTxParams), buildTxParams)
			}

			mRPCService.AssertExpectations(t)
		})
	}
}
