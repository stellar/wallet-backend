package services

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
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

// buildTransactionForTest creates a transaction with the given operations and parameters
func buildTransactionForTest(t *testing.T, operations []txnbuild.Operation, preconditions txnbuild.Preconditions) *txnbuild.Transaction {
	t.Helper()

	// For non-empty operations, create a proper transaction
	sourceAccount := keypair.MustRandom()

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &txnbuild.SimpleAccount{AccountID: sourceAccount.Address(), Sequence: 1},
		IncrementSequenceNum: true,
		Operations:           operations,
		BaseFee:              txnbuild.MinBaseFee,
		Memo:                 nil,
		Preconditions:        preconditions,
	})
	require.NoError(t, err)
	return tx
}

// buildAuthEntry builds a SorobanAuthorizationEntry with the given credential type and signer.
func buildAuthEntry(
	t *testing.T,
	credentialsType xdr.SorobanCredentialsType,
	signerType xdr.ScAddressType,
	signerID string,
) xdr.SorobanAuthorizationEntry {
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
				Signature: xdr.ScVal{Type: xdr.ScValTypeScvVoid},
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
				Signature: xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			},
		}

	case credentialsType == xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount:
		sorobanCredentials = xdr.SorobanCredentials{Type: credentialsType}

	default:
		require.Failf(t, "unsupported credentials or signer type", "credentialsType: %s, signerType: %s", credentialsType, signerType)
	}

	// Build a valid contract address for the auth entry's RootInvocation.
	nativeAssetContractID, err := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(network.TestNetworkPassphrase)
	require.NoError(t, err)
	contractID := xdr.ContractId(nativeAssetContractID)

	return xdr.SorobanAuthorizationEntry{
		Credentials: sorobanCredentials,
		RootInvocation: xdr.SorobanAuthorizedInvocation{
			Function: xdr.SorobanAuthorizedFunction{
				Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn,
				ContractFn: &xdr.InvokeContractArgs{
					ContractAddress: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &contractID,
					},
					FunctionName: "transfer",
				},
			},
		},
	}
}

// buildInvokeContractOpWithSimulation creates an InvokeHostFunction op with Auth and Ext already set,
// simulating the state of an operation after the client has incorporated the simulation response.
func buildInvokeContractOpWithSimulation(
	t *testing.T,
	sorobanTxData xdr.SorobanTransactionData,
	credentialsType xdr.SorobanCredentialsType,
	signerID string,
) *txnbuild.InvokeHostFunction {
	t.Helper()

	op := buildInvokeContractOp(t)
	ext, err := xdr.NewTransactionExt(1, sorobanTxData)
	require.NoError(t, err)
	op.Ext = ext
	op.Auth = []xdr.SorobanAuthorizationEntry{buildAuthEntry(t, credentialsType, xdr.ScAddressTypeScAddressTypeAccount, signerID)}
	return op
}

func TestValidateOptions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	t.Run("return_error_when_db_nil", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: nil,
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			ChannelAccountStore:                &store.ChannelAccountStoreMock{},
			RPCService:                         &RPCServiceMock{},
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
			RPCService:                         &RPCServiceMock{},
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
			RPCService:                         &RPCServiceMock{},
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
			RPCService:                         &RPCServiceMock{},
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
			RPCService:                         &RPCServiceMock{},
			BaseFee:                            txnbuild.MinBaseFee - 10,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "base fee is lower than the minimum network fee", err.Error())
	})
}

func TestBuildAndSignTransactionWithChannelAccount(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, outerErr := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, outerErr)
	defer dbConnectionPool.Close()

	mDistributionAccountSignatureClient := signing.SignatureClientMock{}
	mChannelAccountSignatureClient := signing.SignatureClientMock{}
	mChannelAccountStore := store.ChannelAccountStoreMock{}
	mRPCService := RPCServiceMock{}
	txService, outerErr := NewTransactionService(TransactionServiceOptions{
		DB:                                 dbConnectionPool,
		DistributionAccountSignatureClient: &mDistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      &mChannelAccountSignatureClient,
		ChannelAccountStore:                &mChannelAccountStore,
		RPCService:                         &mRPCService,
		// Use a BaseFee large enough to accommodate typical Soroban ResourceFees under the server-side re-simulation
		// bound. The specific value is not meaningful to any single subtest; it just needs to be > ResourceFee for
		// the Soroban success path to clear the cap.
		BaseFee: 1_000_000,
	})
	require.NoError(t, outerErr)

	t.Run("🔴handle_GetAccountPublicKey_err", func(t *testing.T) {
		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return("", errors.New("channel accounts unavailable")).
			Once()

		signedTx := buildTransactionForTest(t, []txnbuild.Operation{buildPaymentOp(t)}, txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(30),
		})
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), signedTx.ToGenericTransaction())

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

		operations := []txnbuild.Operation{&txnbuild.AccountMerge{
			Destination:   keypair.MustRandom().Address(),
			SourceAccount: channelAccount.Address(),
		}}
		signedTx := buildTransactionForTest(t, operations, txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(30),
		})
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), signedTx.ToGenericTransaction())

		mChannelAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.ErrorContains(t, err, "invalid operation: operation source account cannot be the channel account")
	})

	t.Run("🚨operation_source_account_cannot_be_channel_account_via_muxed_address", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return(channelAccount.Address(), nil).
			Once()

		// Create a muxed M-address from the channel account's G-address.
		muxedAccount, err := xdr.MuxedAccountFromAccountId(channelAccount.Address(), 12345)
		require.NoError(t, err)

		operations := []txnbuild.Operation{&txnbuild.AccountMerge{
			Destination:   keypair.MustRandom().Address(),
			SourceAccount: muxedAccount.Address(),
		}}
		signedTx := buildTransactionForTest(t, operations, txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(30),
		})
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), signedTx.ToGenericTransaction())

		mChannelAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.ErrorContains(t, err, "invalid operation: operation source account cannot be the channel account")
	})

	t.Run("🚨operation_source_account_cannot_be_empty", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return(channelAccount.Address(), nil).
			Once()

		operations := []txnbuild.Operation{&txnbuild.AccountMerge{
			Destination: keypair.MustRandom().Address(),
		}}
		signedTx := buildTransactionForTest(t, operations, txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(30),
		})
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), signedTx.ToGenericTransaction())

		mChannelAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.ErrorContains(t, err, "invalid operation: operation source account cannot be empty for non-Soroban operations")
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

		signedTx := buildTransactionForTest(t, []txnbuild.Operation{buildPaymentOp(t)}, txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(30),
		})
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), signedTx.ToGenericTransaction())

		mChannelAccountSignatureClient.AssertExpectations(t)
		mRPCService.AssertExpectations(t)
		assert.Empty(t, tx)
		expectedErr := fmt.Errorf("getting ledger sequence for channel account public key %q: rpc service down", channelAccount.Address())
		assert.EqualError(t, err, expectedErr.Error())
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

		signedTx := buildTransactionForTest(t, []txnbuild.Operation{buildPaymentOp(t)}, txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(30),
		})
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), signedTx.ToGenericTransaction())

		mChannelAccountSignatureClient.AssertExpectations(t)
		mChannelAccountStore.AssertExpectations(t)
		mRPCService.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.EqualError(t, err, "assigning channel account to tx: unable to assign channel account to tx")
	})

	t.Run("🔴handle_SignStellarTransaction_err_releases_channel_account", func(t *testing.T) {
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
			Once().
			// After signing fails, the channel account must be released so the pool doesn't leak locks on every
			// bad/incomplete build call. Otherwise an attacker flooding buildTransaction can wedge the pool for
			// up to the lock TTL (minutes) with a handful of requests. The ctx here is context.WithoutCancel-wrapped
			// so the unlock still runs when the request ctx is cancelled — mock.Anything matches the wrapped type.
			On("UnassignTxAndUnlockChannelAccounts", mock.Anything, mock.Anything, mock.AnythingOfType("string")).
			Return(int64(1), nil).
			Once()

		mRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()

		operations := []txnbuild.Operation{buildPaymentOp(t)}
		signedTx := buildTransactionForTest(t, operations, txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(30),
		})
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), signedTx.ToGenericTransaction())

		mChannelAccountSignatureClient.AssertExpectations(t)
		mChannelAccountStore.AssertExpectations(t)
		mRPCService.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.EqualError(t, err, "signing transaction with channel account: unable to sign")
	})

	t.Run("🟢build_and_sign_soroban_tx_with_channel_account", func(t *testing.T) {
		sorobanTxDataXDR := "AAAAAAAAAAEAAAAGAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAAFAAAAAEAAAACAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAYAAAAAAAAAABBua0YUOtfPBN8bgJF1VXvNrYCFtsmcg8h+N5Pf2BylAAAAFWhSXFSqNynLAAAAAAAKehUAAAGIAAAA3AAAAAAAAgi1"
		var sorobanTxData xdr.SorobanTransactionData
		err := xdr.SafeUnmarshalBase64(sorobanTxDataXDR, &sorobanTxData)
		require.NoError(t, err)
		require.Equal(t, xdr.Int64(133301), sorobanTxData.ResourceFee)

		op := buildInvokeContractOpWithSimulation(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, keypair.MustRandom().Address())
		signedTx := buildTransactionForTest(t, []txnbuild.Operation{op}, txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(30),
		})
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
			Once().
			On("SimulateTransaction", mock.AnythingOfType("string"), entities.RPCResourceConfig{}).
			Return(entities.RPCSimulateTransactionResult{MinResourceFee: "133301"}, nil).
			Once()

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), signedTx.ToGenericTransaction())

		mChannelAccountSignatureClient.AssertExpectations(t)
		mChannelAccountStore.AssertExpectations(t)
		mRPCService.AssertExpectations(t)
		assert.Equal(t, signedTx, tx)
		assert.NoError(t, err)
	})

	t.Run("🚨reject_soroban_tx_with_channel_account_as_auth_signer", func(t *testing.T) {
		sorobanTxDataXDR := "AAAAAAAAAAEAAAAGAAAAAdeSi3LCcDzP6vfrn/TvTVBKVai5efybRQ6iyEK00c5hAAAAFAAAAAEAAAACAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAYAAAAAAAAAABBua0YUOtfPBN8bgJF1VXvNrYCFtsmcg8h+N5Pf2BylAAAAFWhSXFSqNynLAAAAAAAKehUAAAGIAAAA3AAAAAAAAgi1"
		var sorobanTxData xdr.SorobanTransactionData
		err := xdr.SafeUnmarshalBase64(sorobanTxDataXDR, &sorobanTxData)
		require.NoError(t, err)

		channelAccount := keypair.MustRandom()

		// Build a Soroban tx where the channel account is a signer in the auth entries — this should be rejected.
		op := buildInvokeContractOpWithSimulation(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, channelAccount.Address())
		signedTx := buildTransactionForTest(t, []txnbuild.Operation{op}, txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(30),
		})

		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return(channelAccount.Address(), nil).
			Once()

		mRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), signedTx.ToGenericTransaction())

		mChannelAccountSignatureClient.AssertExpectations(t)
		mRPCService.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.ErrorContains(t, err, sorobanauth.ErrForbiddenSigner.Error())
	})

	t.Run("🟢handle_max_timebound_greater_than_wallet_backend_max_timebound", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		signedTx := buildTransactionForTest(t, []txnbuild.Operation{buildPaymentOp(t)}, txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(500),
		})
		expectedSignedTx := buildTransactionForTest(t, []txnbuild.Operation{buildPaymentOp(t)}, txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		})

		mChannelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background(), 30).
			Return(channelAccount.Address(), nil).
			Once().
			On("NetworkPassphrase").
			Return("networkpassphrase").
			On("SignStellarTransaction", context.Background(), mock.MatchedBy(func(tx *txnbuild.Transaction) bool {
				// Verify that the transaction has been adjusted to have a maximum timeout of 300 seconds
				timeBounds := tx.ToXDR().Preconditions().TimeBounds
				if timeBounds == nil {
					return false
				}
				// Check that MaxTime is set and represents a timeout of approximately 300 seconds from now
				maxTimeoutBounds := txnbuild.NewTimeout(300)
				timeDiff := int64(timeBounds.MaxTime) - maxTimeoutBounds.MaxTime
				// Allow for a small time difference (up to 5 seconds) due to test execution timing
				return timeDiff >= -5 && timeDiff <= 5
			}), []string{channelAccount.Address()}).
			Return(expectedSignedTx, nil).
			Once()

		mChannelAccountStore.
			On("AssignTxToChannelAccount", context.Background(), channelAccount.Address(), mock.AnythingOfType("string")).
			Return(nil).
			Once()

		mRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), signedTx.ToGenericTransaction())

		mChannelAccountSignatureClient.AssertExpectations(t)
		mChannelAccountStore.AssertExpectations(t)
		mRPCService.AssertExpectations(t)
		assert.NotEqual(t, signedTx, tx)
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

	sorobanExt, outerErr := xdr.NewTransactionExt(1, sorobanTxData)
	require.NoError(t, outerErr)

	testCases := []struct {
		name                string
		baseFee             int64
		incomingOps         []txnbuild.Operation
		setupRPCMock        func(t *testing.T, m *RPCServiceMock)
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
			wantErrContains: "invalid Soroban transaction: must have exactly one operation (2 provided)",
		},
		{
			name:    "🔴multiple_ops_where_all_are_soroban",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t),
				buildInvokeContractOp(t),
			},
			wantErrContains: "invalid Soroban transaction: must have exactly one operation (2 provided)",
		},
		{
			name:    "🔴missing_soroban_transaction_data",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOp(t), // no Ext set
			},
			wantErrContains: "missing SorobanTransactionData in operation Ext field",
		},
		{
			name:    "🚨catch_txSource=channelAccount(AuthEntry)",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOpWithSimulation(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, chAccPublicKey),
			},
			wantErrContains: sorobanauth.ErrForbiddenSigner.Error(),
		},
		{
			name:    "🚨catch_txSource=channelAccount(SourceAccount)",
			baseFee: txnbuild.MinBaseFee,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOpWithSimulation(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount, ""),
			},
			wantErrContains: sorobanauth.ErrForbiddenSigner.Error(),
		},
		{
			name:    "🟢successful_InvokeHostFunction_largeBaseFee",
			baseFee: 1000000,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOpWithSimulation(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, keypair.MustRandom().Address()),
			},
			setupRPCMock: func(t *testing.T, m *RPCServiceMock) {
				m.On("SimulateTransaction", mock.AnythingOfType("string"), entities.RPCResourceConfig{}).
					Return(entities.RPCSimulateTransactionResult{MinResourceFee: "133301"}, nil).Once()
			},
			wantBuildTxParamsFn: func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams {
				return txnbuild.TransactionParams{
					Operations: initialBuildTxParams.Operations, // op already has Ext set
					BaseFee:    1000000 - 133301,                // original base fee - soroban fee
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
			name:    "🟢successful_ExtendFootprintTtl_largeBaseFee",
			baseFee: 1000000,
			incomingOps: []txnbuild.Operation{
				&txnbuild.ExtendFootprintTtl{ExtendTo: 1840580937, Ext: sorobanExt},
			},
			setupRPCMock: func(t *testing.T, m *RPCServiceMock) {
				m.On("SimulateTransaction", mock.AnythingOfType("string"), entities.RPCResourceConfig{}).
					Return(entities.RPCSimulateTransactionResult{MinResourceFee: "133301"}, nil).Once()
			},
			wantBuildTxParamsFn: func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams {
				return txnbuild.TransactionParams{
					Operations: initialBuildTxParams.Operations, // op already has Ext set
					BaseFee:    1000000 - 133301,
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
			name:    "🟢successful_RestoreFootprint_largeBaseFee",
			baseFee: 1000000,
			incomingOps: []txnbuild.Operation{
				&txnbuild.RestoreFootprint{Ext: sorobanExt},
			},
			setupRPCMock: func(t *testing.T, m *RPCServiceMock) {
				m.On("SimulateTransaction", mock.AnythingOfType("string"), entities.RPCResourceConfig{}).
					Return(entities.RPCSimulateTransactionResult{MinResourceFee: "133301"}, nil).Once()
			},
			wantBuildTxParamsFn: func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams {
				return txnbuild.TransactionParams{
					Operations: initialBuildTxParams.Operations, // op already has Ext set
					BaseFee:    1000000 - 133301,
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
			name:    "🟢successful_RestoreFootprint_largeBaseFee_signedByContractAuthEntry",
			baseFee: 1000000,
			incomingOps: []txnbuild.Operation{
				&txnbuild.RestoreFootprint{Ext: sorobanExt},
			},
			setupRPCMock: func(t *testing.T, m *RPCServiceMock) {
				m.On("SimulateTransaction", mock.AnythingOfType("string"), entities.RPCResourceConfig{}).
					Return(entities.RPCSimulateTransactionResult{MinResourceFee: "133301"}, nil).Once()
			},
			wantBuildTxParamsFn: func(t *testing.T, initialBuildTxParams txnbuild.TransactionParams) txnbuild.TransactionParams {
				return txnbuild.TransactionParams{
					Operations: initialBuildTxParams.Operations, // op already has Ext set
					BaseFee:    1000000 - 133301,
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
			name:    "🚨rejects_inflated_ResourceFee_above_server_resim_bound",
			baseFee: 10_000_000,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOpWithSimulation(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, keypair.MustRandom().Address()),
			},
			// Client-declared ResourceFee is 133301 (inside sorobanTxData), but server re-sim reports a MinResourceFee
			// of "50000". With safety_factor=2, maxAllowed = min(100000, 10_000_000) = 100000. 133301 > 100000 → reject.
			setupRPCMock: func(t *testing.T, m *RPCServiceMock) {
				m.On("SimulateTransaction", mock.AnythingOfType("string"), entities.RPCResourceConfig{}).
					Return(entities.RPCSimulateTransactionResult{MinResourceFee: "50000"}, nil).Once()
			},
			wantErrContains: "resource fee",
		},
		{
			name:    "🚨rejects_ResourceFee_above_base_fee_cap",
			baseFee: 100_000,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOpWithSimulation(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, keypair.MustRandom().Address()),
			},
			// Client-declared ResourceFee = 133301. baseFee cap = 100000. Server returns high MinResourceFee that
			// would *allow* it via safety factor, but hard cap at t.BaseFee should still reject.
			setupRPCMock: func(t *testing.T, m *RPCServiceMock) {
				m.On("SimulateTransaction", mock.AnythingOfType("string"), entities.RPCResourceConfig{}).
					Return(entities.RPCSimulateTransactionResult{MinResourceFee: "133301"}, nil).Once()
			},
			wantErrContains: "resource fee",
		},
		{
			name:    "🚨rejects_when_server_resim_returns_payload_error",
			baseFee: 1_000_000,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOpWithSimulation(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, keypair.MustRandom().Address()),
			},
			setupRPCMock: func(t *testing.T, m *RPCServiceMock) {
				m.On("SimulateTransaction", mock.AnythingOfType("string"), entities.RPCResourceConfig{}).
					Return(entities.RPCSimulateTransactionResult{Error: "server said no"}, nil).Once()
			},
			wantErrContains: "server-side re-simulation failed",
		},
		{
			name:    "🚨rejects_when_server_resim_rpc_error",
			baseFee: 1_000_000,
			incomingOps: []txnbuild.Operation{
				buildInvokeContractOpWithSimulation(t, sorobanTxData, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, keypair.MustRandom().Address()),
			},
			setupRPCMock: func(t *testing.T, m *RPCServiceMock) {
				m.On("SimulateTransaction", mock.AnythingOfType("string"), entities.RPCResourceConfig{}).
					Return(entities.RPCSimulateTransactionResult{}, errors.New("rpc down")).Once()
			},
			wantErrContains: "server-side re-simulation",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mRPCService := &RPCServiceMock{}
			if tc.setupRPCMock != nil {
				tc.setupRPCMock(t, mRPCService)
			}
			defer mRPCService.AssertExpectations(t)

			txService := &transactionService{
				BaseFee:    tc.baseFee,
				RPCService: mRPCService,
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

			buildTxParams, err := txService.adjustParamsForSoroban(context.Background(), chAccPublicKey, incomingBuildTxParams)
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
