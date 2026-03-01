package services

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/txnbuild"
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

	mockMetricsService := metrics.NewMockMetricsService()

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
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

		mockMetricsService.On("ObserveDBQueryDuration", "IsAccountFeeBumpEligible", "channel_accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "IsAccountFeeBumpEligible", "channel_accounts").Once()
		defer mockMetricsService.AssertExpectations(t)

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

		mockMetricsService.On("ObserveDBQueryDuration", "IsAccountFeeBumpEligible", "channel_accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "IsAccountFeeBumpEligible", "channel_accounts").Once()
		defer mockMetricsService.AssertExpectations(t)

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

		mockMetricsService.On("ObserveDBQueryDuration", "IsAccountFeeBumpEligible", "channel_accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "IsAccountFeeBumpEligible", "channel_accounts").Once()
		defer mockMetricsService.AssertExpectations(t)

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

		mockMetricsService.On("ObserveDBQueryDuration", "IsAccountFeeBumpEligible", "channel_accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "IsAccountFeeBumpEligible", "channel_accounts").Once()
		defer mockMetricsService.AssertExpectations(t)

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
}
