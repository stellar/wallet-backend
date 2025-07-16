package services

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/strkey"
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
	"github.com/stellar/wallet-backend/internal/transactions/utils"
	"github.com/stellar/wallet-backend/pkg/sorobanauth"
)

func buildInvokeContractOp(t *testing.T) *txnbuild.InvokeHostFunction {
	t.Helper()

	var nativeAssetContractID xdr.ContractId
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

// buildSimulationResponse is a helper function that returns a simulation response with one auth entry with the specified credential type.
func buildSimulationResponse(
	t *testing.T,
	sorobanTxData xdr.SorobanTransactionData,
	credentialsType xdr.SorobanCredentialsType,
	signerType xdr.ScAddressType,
	signerID string,
) entities.RPCSimulateTransactionResult {
	t.Helper()

	var sorobanCredentials xdr.SorobanCredentials

	switch {
	case credentialsType == xdr.SorobanCredentialsTypeSorobanCredentialsAddress && signerType == xdr.ScAddressTypeScAddressTypeAccount:
		authEntryAccountID, err := xdr.AddressToAccountId(signerID)
		require.NoError(t, err)
		sorobanCredentials = xdr.SorobanCredentials{
			Type: credentialsType,
			Address: &xdr.SorobanAddressCredentials{
				Address: xdr.ScAddress{
					Type:      signerType,
					AccountId: &authEntryAccountID,
				},
			},
		}

	case credentialsType == xdr.SorobanCredentialsTypeSorobanCredentialsAddress && signerType == xdr.ScAddressTypeScAddressTypeContract:
		decodedContractID, err := strkey.Decode(strkey.VersionByteContract, signerID)
		require.NoError(t, err)
		authEntryContractID := xdr.ContractId(decodedContractID)
		sorobanCredentials = xdr.SorobanCredentials{
			Type: credentialsType,
			Address: &xdr.SorobanAddressCredentials{
				Address: xdr.ScAddress{
					Type:       signerType,
					ContractId: &authEntryContractID,
				},
			},
		}

	case credentialsType == xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount:
		sorobanCredentials = xdr.SorobanCredentials{Type: credentialsType}

	default:
		require.Failf(t, "unsupported credentials or signer type", "credentialsType: %s, signerType: %s", credentialsType, signerType)
	}

	return entities.RPCSimulateTransactionResult{
		TransactionData: sorobanTxData,
		Results: []entities.RPCSimulateHostFunctionResult{
			{Auth: []xdr.SorobanAuthorizationEntry{{Credentials: sorobanCredentials}}},
		},
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
	dbConnectionPool, outerErr := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, outerErr)
	defer dbConnectionPool.Close()

	mDistributionAccountSignatureClient := signing.SignatureClientMock{}
	mChannelAccountSignatureClient := signing.SignatureClientMock{}
	mChannelAccountStore := store.ChannelAccountStoreMock{}
	mRPCService := services.RPCServiceMock{}
	txService, outerErr := NewTransactionService(TransactionServiceOptions{
		DB:                                 dbConnectionPool,
		DistributionAccountSignatureClient: &mDistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      &mChannelAccountSignatureClient,
		ChannelAccountStore:                &mChannelAccountStore,
		RPCService:                         &mRPCService,
		BaseFee:                            114,
	})
	require.NoError(t, outerErr)

	t.Run("🔴timeout_must_be_smaller_than_max_timeout", func(t *testing.T) {
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, MaxTimeoutInSeconds+1, entities.RPCSimulateTransactionResult{})
		assert.Empty(t, tx)
		assert.ErrorContains(t, err, fmt.Sprintf("cannot be greater than %d seconds", MaxTimeoutInSeconds))
	})

	t.Run("🔴handle_GetAccountPublicKey_err", func(t *testing.T) {
		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return("", errors.New("channel accounts unavailable")).
			Once()

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, 30, entities.RPCSimulateTransactionResult{})

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
		}}, 30, entities.RPCSimulateTransactionResult{})

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
		}}, 30, entities.RPCSimulateTransactionResult{})

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

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, 30, entities.RPCSimulateTransactionResult{})

		mChannelAccountSignatureClient.AssertExpectations(t)
		mRPCService.AssertExpectations(t)
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

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, 30, entities.RPCSimulateTransactionResult{})

		mChannelAccountSignatureClient.AssertExpectations(t)
		mRPCService.AssertExpectations(t)
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

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{buildPaymentOp(t)}, 30, entities.RPCSimulateTransactionResult{})

		mChannelAccountSignatureClient.AssertExpectations(t)
		mChannelAccountStore.AssertExpectations(t)
		mRPCService.AssertExpectations(t)
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

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{buildPaymentOp(t)}, 30, entities.RPCSimulateTransactionResult{})

		mChannelAccountSignatureClient.AssertExpectations(t)
		mChannelAccountStore.AssertExpectations(t)
		mRPCService.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.EqualError(t, err, "signing transaction with channel account: unable to sign")
	})

	t.Run("🟢build_and_sign_classic_tx_with_channel_account", func(t *testing.T) {
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

				tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{buildPaymentOp(t)}, int64(tc.inputTimeout), entities.RPCSimulateTransactionResult{})

				mChannelAccountSignatureClient.AssertExpectations(t)
				mChannelAccountStore.AssertExpectations(t)
				mRPCService.AssertExpectations(t)
				assert.Equal(t, signedTx, tx)
				assert.NoError(t, err)
			})
		}
	})

	t.Run("🟢build_and_sign_soroban_tx_with_channel_account", func(t *testing.T) {
		signedTx := utils.BuildTestTransaction(t)
		channelAccount := keypair.MustRandom()

		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
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

		sorobanTxDataXDR := "AAAAAAAAAAEAAAAGAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAAFAAAAAEAAAACAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAYAAAAAAAAAABBua0YUOtfPBN8bgJF1VXvNrYCFtsmcg8h+N5Pf2BylAAAAFWhSXFSqNynLAAAAAAAKehUAAAGIAAAA3AAAAAAAAgi1"
		var sorobanTxData xdr.SorobanTransactionData
		err := xdr.SafeUnmarshalBase64(sorobanTxDataXDR, &sorobanTxData)
		require.NoError(t, err)
		require.Equal(t, xdr.Int64(133301), sorobanTxData.ResourceFee)

		simulationResponse := buildSimulationResponse(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, xdr.ScAddressTypeScAddressTypeAccount, keypair.MustRandom().Address())
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{buildInvokeContractOp(t)}, 30, simulationResponse)

		mChannelAccountSignatureClient.AssertExpectations(t)
		mChannelAccountStore.AssertExpectations(t)
		mRPCService.AssertExpectations(t)
		assert.Equal(t, signedTx, tx)
		assert.NoError(t, err)
	})
}

func Test_transactionService_adjustParamsForSoroban(t *testing.T) {
	const chAccPublicKey = "GAB3B3MFQHSEN4YOTZBKKEL7VTWJQ6SWOWAE36I7CWZP3I3A3VT464KG"
	txSourceAccount := keypair.MustRandom().Address()

	sorobanTxDataXDR := "AAAAAAAAAAEAAAAGAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAAFAAAAAEAAAACAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAYAAAAAAAAAABBua0YUOtfPBN8bgJF1VXvNrYCFtsmcg8h+N5Pf2BylAAAAFWhSXFSqNynLAAAAAAAKehUAAAGIAAAA3AAAAAAAAgi1"
	var sorobanTxData xdr.SorobanTransactionData
	outerErr := xdr.SafeUnmarshalBase64(sorobanTxDataXDR, &sorobanTxData)
	require.NoError(t, outerErr)
	require.Equal(t, xdr.Int64(133301), sorobanTxData.ResourceFee)

	testCases := []struct {
		name                string
		baseFee             int64
		incomingOps         []txnbuild.Operation
		simulationResponse  entities.RPCSimulateTransactionResult
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
			wantErrContains: "Soroban transactions require exactly one operation but 2 were provided",
		},
		{
			name:    "🔴multiple_ops_where_all_are_soroban",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
				buildInvokeContractOp(t),
			},
			wantErrContains: "Soroban transactions require exactly one operation but 2 were provided",
		},
		{
			name:    "🔴handle_simulateTransaction_err",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
			},
			simulationResponse: entities.RPCSimulateTransactionResult{},
			wantErrContains:    "invalid arguments: simulation response cannot be empty",
		},
		{
			name:    "🔴handle_simulateTransaction_error_in_payload",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
			},
			simulationResponse: entities.RPCSimulateTransactionResult{
				Error: "simulate transaction failed because fooBar",
			},
			wantErrContains: "transaction simulation failed with error=simulate transaction failed because fooBar",
		},
		{
			name:    "🚨catch_txSource=channelAccount(AuthEntry)",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
			},
			simulationResponse: buildSimulationResponse(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, xdr.ScAddressTypeScAddressTypeAccount, chAccPublicKey),
			wantErrContains:    sorobanauth.ErrForbiddenSigner.Error(),
		},
		{
			name:    "🚨catch_txSource=channelAccount(SourceAccount)",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
			},
			simulationResponse: buildSimulationResponse(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount, 0, ""),
			wantErrContains:    sorobanauth.ErrForbiddenSigner.Error(),
		},
		{
			name:    "🟢successful_InvokeHostFunction_largeBaseFee",
			baseFee: 1000000,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
			},
			simulationResponse: buildSimulationResponse(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, xdr.ScAddressTypeScAddressTypeAccount, keypair.MustRandom().Address()),
			wantBuildTxParamsFn: func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams {
				newInvokeContractOp := buildInvokeContractOp(t)
				require.Empty(t, newInvokeContractOp.Ext)
				var err error
				newInvokeContractOp.Ext, err = xdr.NewTransactionExt(1, sorobanTxData)
				require.NoError(t, err)

				return txnbuild.TransactionParams{
					Operations: []txnbuild.Operation{newInvokeContractOp},
					BaseFee:    1000000 - 133301, // original base fee - soroban fee
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
			name:    "🟢successful_InvokeHostFunction_minBaseFee",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
			},
			simulationResponse: buildSimulationResponse(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, xdr.ScAddressTypeScAddressTypeAccount, keypair.MustRandom().Address()),
			wantBuildTxParamsFn: func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams {
				newInvokeContractOp := buildInvokeContractOp(t)
				require.Empty(t, newInvokeContractOp.Ext)
				var err error
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
		{
			name:    "🟢successful_ExtendFootprintTtl_minBaseFee",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				&txnbuild.ExtendFootprintTtl{ExtendTo: 1840580937},
			},
			simulationResponse: buildSimulationResponse(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, xdr.ScAddressTypeScAddressTypeAccount, keypair.MustRandom().Address()),
			wantBuildTxParamsFn: func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams {
				newExtendFootprintTTLOp := &txnbuild.ExtendFootprintTtl{ExtendTo: 1840580937}
				var err error
				newExtendFootprintTTLOp.Ext, err = xdr.NewTransactionExt(1, sorobanTxData)
				require.NoError(t, err)

				return txnbuild.TransactionParams{
					Operations: []txnbuild.Operation{newExtendFootprintTTLOp},
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
		{
			name:    "🟢successful_RestoreFootprint_minBaseFee",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				&txnbuild.RestoreFootprint{},
			},
			simulationResponse: buildSimulationResponse(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, xdr.ScAddressTypeScAddressTypeAccount, keypair.MustRandom().Address()),
			wantBuildTxParamsFn: func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams {
				newRestoreFootprintOp := &txnbuild.RestoreFootprint{}
				var err error
				newRestoreFootprintOp.Ext, err = xdr.NewTransactionExt(1, sorobanTxData)
				require.NoError(t, err)

				return txnbuild.TransactionParams{
					Operations: []txnbuild.Operation{newRestoreFootprintOp},
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
		{
			name:    "🟢successful_RestoreFootprint_minBaseFee_signedByContractAuthEntry",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				&txnbuild.RestoreFootprint{},
			},
			simulationResponse: buildSimulationResponse(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, xdr.ScAddressTypeScAddressTypeContract, "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"),
			wantBuildTxParamsFn: func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams {
				newRestoreFootprintOp := &txnbuild.RestoreFootprint{}
				var err error
				newRestoreFootprintOp.Ext, err = xdr.NewTransactionExt(1, sorobanTxData)
				require.NoError(t, err)

				return txnbuild.TransactionParams{
					Operations: []txnbuild.Operation{newRestoreFootprintOp},
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
			txService := &transactionService{
				BaseFee: tc.baseFee,
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

			buildTxParams, err := txService.adjustParamsForSoroban(context.Background(), chAccPublicKey, incomingBuildTxParams, tc.simulationResponse)
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				assert.Empty(t, buildTxParams)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.wantBuildTxParamsFn(t, incomingBuildTxParams), buildTxParams)
			}
		})
	}
}
