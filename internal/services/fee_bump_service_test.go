package services

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/signing"
)

func TestFeeBumpServiceWrapTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := metrics.NewMetrics(prometheus.NewRegistry())

	models, err := data.NewModels(dbConnectionPool, m.DB)
	require.NoError(t, err)

	signatureClient := signing.SignatureClientMock{}
	defer signatureClient.AssertExpectations(t)
	s, err := NewFeeBumpService(FeeBumpServiceOptions{
		DistributionAccountSignatureClient: &signatureClient,
		BaseFee:                            txnbuild.MinBaseFee,
		Models:                             models,
	})
	require.NoError(t, err)

	t.Run("account_not_eligible_for_transaction_fee_bump", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()

		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: accountToSponsor.Address(),
				Sequence:  123,
			},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Amount:      "10",
					Asset:       txnbuild.NativeAsset{},
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		feeBumpTxe, networkPassphrase, err := s.WrapTransaction(ctx, tx)
		assert.ErrorIs(t, ErrAccountNotEligibleForBeingSponsored, err)
		assert.Empty(t, feeBumpTxe)
		assert.Empty(t, networkPassphrase)
	})

	t.Run("transaction_fee_exceeds_maximum_base_fee_for_sponsoring", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()

		// Insert into channel_accounts to make account fee-bump eligible
		_, err := dbConnectionPool.Exec(ctx, "INSERT INTO channel_accounts (public_key, encrypted_private_key) VALUES ($1, 'encrypted')", accountToSponsor.Address())
		require.NoError(t, err)

		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: accountToSponsor.Address(),
				Sequence:  123,
			},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Amount:      "10",
					Asset:       txnbuild.NativeAsset{},
				},
			},
			BaseFee:       100 * txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		feeBumpTxe, networkPassphrase, err := s.WrapTransaction(ctx, tx)
		assert.ErrorIs(t, err, ErrFeeExceedsMaximumBaseFee)
		assert.Empty(t, feeBumpTxe)
		assert.Empty(t, networkPassphrase)
	})

	t.Run("transaction_should_have_at_least_one_signature", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()

		// Insert into channel_accounts to make account fee-bump eligible
		_, err := dbConnectionPool.Exec(ctx, "INSERT INTO channel_accounts (public_key, encrypted_private_key) VALUES ($1, 'encrypted')", accountToSponsor.Address())
		require.NoError(t, err)

		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: accountToSponsor.Address(),
				Sequence:  123,
			},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Amount:      "10",
					Asset:       txnbuild.NativeAsset{},
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		feeBumpTxe, networkPassphrase, err := s.WrapTransaction(ctx, tx)
		assert.ErrorIs(t, err, ErrNoSignaturesProvided)
		assert.Empty(t, feeBumpTxe)
		assert.Empty(t, networkPassphrase)
	})

	t.Run("successfully_wraps_the_transaction_with_fee_bump", func(t *testing.T) {
		distributionAccount := keypair.MustRandom()
		accountToSponsor := keypair.MustRandom()

		// Insert into channel_accounts to make account fee-bump eligible
		_, err := dbConnectionPool.Exec(ctx, "INSERT INTO channel_accounts (public_key, encrypted_private_key) VALUES ($1, 'encrypted')", accountToSponsor.Address())
		require.NoError(t, err)

		destinationAccount := keypair.MustRandom().Address()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: accountToSponsor.Address(),
				Sequence:  123,
			},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: destinationAccount,
					Amount:      "10",
					Asset:       txnbuild.NativeAsset{},
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		tx, err = tx.Sign(network.TestNetworkPassphrase, accountToSponsor)
		require.NoError(t, err)

		signedFeeBumpTx := txnbuild.FeeBumpTransaction{}
		signatureClient.
			On("GetAccountPublicKey", ctx).
			Return(distributionAccount.Address(), nil).
			Once().
			On("NetworkPassphrase").
			Return(network.TestNetworkPassphrase).
			Once().
			On("SignStellarFeeBumpTransaction", ctx, mock.AnythingOfType("*txnbuild.FeeBumpTransaction")).
			Run(func(args mock.Arguments) {
				feeBumpTx, ok := args.Get(1).(*txnbuild.FeeBumpTransaction)
				require.True(t, ok)

				assert.Equal(t, distributionAccount.Address(), feeBumpTx.FeeAccount())
				assert.Equal(t, []txnbuild.Operation{
					&txnbuild.Payment{
						Destination: destinationAccount,
						Amount:      "10",
						Asset:       txnbuild.NativeAsset{},
					},
				}, feeBumpTx.InnerTransaction().Operations())

				feeBumpTx, err = feeBumpTx.Sign(network.TestNetworkPassphrase, distributionAccount)
				require.NoError(t, err)

				signedFeeBumpTx = *feeBumpTx
			}).
			Return(&signedFeeBumpTx, nil)

		feeBumpTxe, networkPassphrase, err := s.WrapTransaction(ctx, tx)
		require.NoError(t, err)

		assert.Equal(t, network.TestNetworkPassphrase, networkPassphrase)
		assert.NotEmpty(t, feeBumpTxe)
		genericTx, err := txnbuild.TransactionFromXDR(feeBumpTxe)
		require.NoError(t, err)
		feeBumpTx, ok := genericTx.FeeBump()
		require.True(t, ok)
		assert.Equal(t, distributionAccount.Address(), feeBumpTx.FeeAccount())
		assert.Len(t, feeBumpTx.InnerTransaction().Operations(), 1)
		assert.Len(t, feeBumpTx.Signatures(), 1)
	})

	t.Run("🚨soroban_inner_with_inflated_resource_fee_is_rejected", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()

		// Insert into channel_accounts to make account fee-bump eligible
		_, err := dbConnectionPool.Exec(ctx, "INSERT INTO channel_accounts (public_key, encrypted_private_key) VALUES ($1, 'encrypted')", accountToSponsor.Address())
		require.NoError(t, err)

		// Build a Soroban InvokeHostFunction op whose transaction-level Ext declares a ResourceFee of 500 — well above
		// s.BaseFee (MinBaseFee = 100) times the inner op count. The network would reject the fee-bump envelope for
		// insufficient fee at submit time; the wallet-backend must reject it earlier so the sponsor never signs.
		nativeAssetContractIDBytes, err := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(network.TestNetworkPassphrase)
		require.NoError(t, err)
		nativeAssetContractID := xdr.ContractId(nativeAssetContractIDBytes)
		invokeOp := &txnbuild.InvokeHostFunction{
			HostFunction: xdr.HostFunction{
				Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
				InvokeContract: &xdr.InvokeContractArgs{
					ContractAddress: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &nativeAssetContractID,
					},
					FunctionName: "noop",
					Args:         xdr.ScVec{},
				},
			},
			SourceAccount: accountToSponsor.Address(),
		}
		sorobanData := xdr.SorobanTransactionData{
			Resources: xdr.SorobanResources{
				Footprint: xdr.LedgerFootprint{},
			},
			ResourceFee: 500,
		}
		invokeOp.Ext, err = xdr.NewTransactionExt(1, sorobanData)
		require.NoError(t, err)

		// Inner base fee set to MinBaseFee (100). After adjustParamsForSoroban would normally subtract ResourceFee,
		// but here we construct the tx directly — simulating a raw client submission that bypasses the backend's
		// build path.
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: accountToSponsor.Address(),
				Sequence:  123,
			},
			IncrementSequenceNum: true,
			Operations:           []txnbuild.Operation{invokeOp},
			BaseFee:              txnbuild.MinBaseFee,
			Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		tx, err = tx.Sign(network.TestNetworkPassphrase, accountToSponsor)
		require.NoError(t, err)

		feeBumpTxe, networkPassphrase, err := s.WrapTransaction(ctx, tx)
		assert.ErrorIs(t, err, ErrFeeExceedsMaximumBaseFee)
		assert.Empty(t, feeBumpTxe)
		assert.Empty(t, networkPassphrase)
	})
}
