package services

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/signing"
)

func TestAccountSponsorshipServiceSponsorAccountCreationTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	models, err := data.NewModels(dbConnectionPool)
	require.NoError(t, err)

	signatureClient := signing.SignatureClientMock{}
	defer signatureClient.AssertExpectations(t)
	mockRPCService := RPCServiceMock{}

	ctx := context.Background()
	s, err := NewAccountSponsorshipService(AccountSponsorshipServiceOptions{
		DistributionAccountSignatureClient: &signatureClient,
		ChannelAccountSignatureClient:      &signatureClient,
		RPCService:                         &mockRPCService,
		MaxSponsoredBaseReserves:           10,
		BaseFee:                            txnbuild.MinBaseFee,
		Models:                             models,
		BlockedOperationsTypes:             []xdr.OperationType{},
	})
	require.NoError(t, err)

	t.Run("account_already_exists", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom().Address()

		mockRPCService.
			On("GetAccountLedgerSequence", accountToSponsor).
			Return(int64(1), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		txe, networkPassphrase, err := s.SponsorAccountCreationTransaction(ctx, accountToSponsor, []entities.Signer{}, []entities.Asset{})
		assert.ErrorIs(t, ErrAccountAlreadyExists, err)
		assert.Empty(t, txe)
		assert.Empty(t, networkPassphrase)
	})

	t.Run("invalid_signers_weight", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom().Address()

		mockRPCService.
			On("GetAccountLedgerSequence", accountToSponsor).
			Return(int64(0), errors.New(accountNotFoundError)).
			Once()
		defer mockRPCService.AssertExpectations(t)

		signers := []entities.Signer{
			{
				Address: keypair.MustRandom().Address(),
				Weight:  0,
				Type:    entities.PartialSignerType,
			},
		}

		txe, networkPassphrase, err := s.SponsorAccountCreationTransaction(ctx, accountToSponsor, signers, []entities.Asset{})
		assert.EqualError(t, err, "no full signers provided")
		assert.Empty(t, txe)
		assert.Empty(t, networkPassphrase)
	})

	t.Run("sponsorship_limit_reached", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom().Address()

		mockRPCService.
			On("GetAccountLedgerSequence", accountToSponsor).
			Return(int64(0), errors.New(accountNotFoundError)).
			Once()
		defer mockRPCService.AssertExpectations(t)

		signers := []entities.Signer{
			{
				Address: keypair.MustRandom().Address(),
				Weight:  10,
				Type:    entities.PartialSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  10,
				Type:    entities.PartialSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  10,
				Type:    entities.PartialSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  10,
				Type:    entities.PartialSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  10,
				Type:    entities.PartialSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  10,
				Type:    entities.PartialSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  10,
				Type:    entities.PartialSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  20,
				Type:    entities.FullSignerType,
			},
		}

		assets := []entities.Asset{
			{
				Code:   "USDC",
				Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
			},
			{
				Code:   "ARST",
				Issuer: "GB7TAYRUZGE6TVT7NHP5SMIZRNQA6PLM423EYISAOAP3MKYIQMVYP2JO",
			},
		}

		txe, networkPassphrase, err := s.SponsorAccountCreationTransaction(ctx, accountToSponsor, signers, assets)
		assert.ErrorIs(t, ErrSponsorshipLimitExceeded, err)
		assert.Empty(t, txe)
		assert.Empty(t, networkPassphrase)
	})

	t.Run("successfully_returns_a_sponsored_transaction", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom().Address()
		distributionAccount := keypair.MustRandom()

		signers := []entities.Signer{
			{
				Address: keypair.MustRandom().Address(),
				Weight:  10,
				Type:    entities.PartialSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  10,
				Type:    entities.PartialSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  20,
				Type:    entities.FullSignerType,
			},
		}

		assets := []entities.Asset{
			{
				Code:   "USDC",
				Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
			},
			{
				Code:   "ARST",
				Issuer: "GB7TAYRUZGE6TVT7NHP5SMIZRNQA6PLM423EYISAOAP3MKYIQMVYP2JO",
			},
		}

		mockRPCService.
			On("GetAccountLedgerSequence", accountToSponsor).
			Return(int64(0), errors.New(accountNotFoundError)).
			Once().
			On("GetAccountLedgerSequence", distributionAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		signedTx := txnbuild.Transaction{}
		signatureClient.
			On("GetAccountPublicKey", ctx).
			Return(distributionAccount.Address(), nil).
			Times(2).
			On("NetworkPassphrase").
			Return(network.TestNetworkPassphrase).
			Once().
			On("SignStellarTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
			Run(func(args mock.Arguments) {
				tx, ok := args.Get(1).(*txnbuild.Transaction)
				require.True(t, ok)

				// We make this workaround because the signers' SetOptions has some inner data that can't be asserted.
				txe, err := tx.Base64()
				require.NoError(t, err)
				genericTx, err := txnbuild.TransactionFromXDR(txe)
				require.NoError(t, err)
				tx, ok = genericTx.Transaction()
				require.True(t, ok)

				assert.Equal(t, distributionAccount.Address(), tx.SourceAccount().AccountID)
				assert.Equal(t, txnbuild.NewTimeout(CreateAccountTxnTimeBounds+CreateAccountTxnTimeBoundsSafetyMargin), tx.Timebounds())
				assert.Equal(t, []txnbuild.Operation{
					&txnbuild.BeginSponsoringFutureReserves{
						SponsoredID:   accountToSponsor,
						SourceAccount: distributionAccount.Address(),
					},
					&txnbuild.CreateAccount{
						Destination:   accountToSponsor,
						Amount:        "0.0000000",
						SourceAccount: distributionAccount.Address(),
					},
					&txnbuild.SetOptions{
						Signer:        &txnbuild.Signer{Address: signers[0].Address, Weight: txnbuild.Threshold(signers[0].Weight)},
						SourceAccount: accountToSponsor,
					},
					&txnbuild.SetOptions{
						Signer:        &txnbuild.Signer{Address: signers[1].Address, Weight: txnbuild.Threshold(signers[1].Weight)},
						SourceAccount: accountToSponsor,
					},
					&txnbuild.SetOptions{
						Signer:        &txnbuild.Signer{Address: signers[2].Address, Weight: txnbuild.Threshold(signers[2].Weight)},
						SourceAccount: accountToSponsor,
					},
					&txnbuild.ChangeTrust{
						Line: txnbuild.CreditAsset{
							Code:   assets[0].Code,
							Issuer: assets[0].Issuer,
						}.MustToChangeTrustAsset(),
						Limit:         "922337203685.4775807",
						SourceAccount: accountToSponsor,
					},
					&txnbuild.ChangeTrust{
						Line: txnbuild.CreditAsset{
							Code:   assets[1].Code,
							Issuer: assets[1].Issuer,
						}.MustToChangeTrustAsset(),
						Limit:         "922337203685.4775807",
						SourceAccount: accountToSponsor,
					},
					&txnbuild.EndSponsoringFutureReserves{
						SourceAccount: accountToSponsor,
					},
					&txnbuild.SetOptions{
						MasterWeight:    txnbuild.NewThreshold(0),
						LowThreshold:    txnbuild.NewThreshold(20),
						MediumThreshold: txnbuild.NewThreshold(20),
						HighThreshold:   txnbuild.NewThreshold(20),
						SourceAccount:   accountToSponsor,
					},
				}, tx.Operations())

				tx, err = tx.Sign(network.TestNetworkPassphrase, distributionAccount)
				require.NoError(t, err)

				signedTx = *tx
			}).
			Return(&signedTx, nil).
			Once().
			On("SignStellarTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
			Return(&signedTx, nil).
			Once()

		txe, networkPassphrase, err := s.SponsorAccountCreationTransaction(ctx, accountToSponsor, signers, assets)
		require.NoError(t, err)

		assert.Equal(t, network.TestNetworkPassphrase, networkPassphrase)
		assert.NotEmpty(t, txe)
		genericTx, err := txnbuild.TransactionFromXDR(txe)
		require.NoError(t, err)
		tx, ok := genericTx.Transaction()
		require.True(t, ok)
		assert.Len(t, tx.Operations(), 9)
		assert.Len(t, tx.Signatures(), 1)

		isFeeBumpEligible, err := models.Account.IsAccountFeeBumpEligible(ctx, accountToSponsor)
		require.NoError(t, err)
		assert.True(t, isFeeBumpEligible)
	})
}

func TestAccountSponsorshipServiceWrapTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	models, err := data.NewModels(dbConnectionPool)
	require.NoError(t, err)

	signatureClient := signing.SignatureClientMock{}
	defer signatureClient.AssertExpectations(t)

	mockRPCService := RPCServiceMock{}

	ctx := context.Background()
	s, err := NewAccountSponsorshipService(AccountSponsorshipServiceOptions{
		DistributionAccountSignatureClient: &signatureClient,
		ChannelAccountSignatureClient:      &signatureClient,
		RPCService:                         &mockRPCService,
		MaxSponsoredBaseReserves:           10,
		BaseFee:                            txnbuild.MinBaseFee,
		Models:                             models,
		BlockedOperationsTypes:             []xdr.OperationType{xdr.OperationTypeLiquidityPoolDeposit},
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
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(CreateAccountTxnTimeBounds + CreateAccountTxnTimeBoundsSafetyMargin)},
		})
		require.NoError(t, err)

		feeBumpTxe, networkPassphrase, err := s.WrapTransaction(ctx, tx)
		assert.ErrorIs(t, ErrAccountNotEligibleForBeingSponsored, err)
		assert.Empty(t, feeBumpTxe)
		assert.Empty(t, networkPassphrase)
	})

	t.Run("blocked_operations", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()
		err := models.Account.Insert(ctx, accountToSponsor.Address())
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
				&txnbuild.LiquidityPoolDeposit{
					LiquidityPoolID: txnbuild.LiquidityPoolId{123},
					MaxAmountA:      "100",
					MaxAmountB:      "200",
					MinPrice:        xdr.Price{N: 1, D: 1},
					MaxPrice:        xdr.Price{N: 1, D: 1},
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(CreateAccountTxnTimeBounds + CreateAccountTxnTimeBoundsSafetyMargin)},
		})
		require.NoError(t, err)

		feeBumpTxe, networkPassphrase, err := s.WrapTransaction(ctx, tx)
		var errOperationNotAllowed *ErrOperationNotAllowed
		assert.ErrorAs(t, err, &errOperationNotAllowed)
		assert.Empty(t, feeBumpTxe)
		assert.Empty(t, networkPassphrase)
	})

	t.Run("transaction_fee_exceeds_maximum_base_fee_for_sponsoring", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()
		err := models.Account.Insert(ctx, accountToSponsor.Address())
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
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(CreateAccountTxnTimeBounds + CreateAccountTxnTimeBoundsSafetyMargin)},
		})
		require.NoError(t, err)

		feeBumpTxe, networkPassphrase, err := s.WrapTransaction(ctx, tx)
		assert.ErrorIs(t, err, ErrFeeExceedsMaximumBaseFee)
		assert.Empty(t, feeBumpTxe)
		assert.Empty(t, networkPassphrase)
	})

	t.Run("transaction_should_have_at_least_one_signature", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()
		err := models.Account.Insert(ctx, accountToSponsor.Address())
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
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(CreateAccountTxnTimeBounds + CreateAccountTxnTimeBoundsSafetyMargin)},
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
		err := models.Account.Insert(ctx, accountToSponsor.Address())
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
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(CreateAccountTxnTimeBounds + CreateAccountTxnTimeBoundsSafetyMargin)},
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
