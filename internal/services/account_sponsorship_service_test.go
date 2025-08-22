package services

import (
	"context"
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
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/utils"
)

func Test_AccountSponsorshipService_SponsorAccountCreationTransaction_failure(t *testing.T) {
	const maxSponsoredBaseReserves = 5
	ctx := context.Background()
	masterKP := keypair.MustRandom()

	testCases := []struct {
		name            string
		prepareMocks    func(t *testing.T, mockRPCService *RPCServiceMock)
		opts            SponsorAccountCreationOptions
		wantErrIs       error
		wantErrContains string
	}{
		{
			name: "🔴account_already_exists",
			opts: SponsorAccountCreationOptions{
				Address: masterKP.Address(),
			},
			prepareMocks: func(t *testing.T, mockRPCService *RPCServiceMock) {
				mockRPCService.
					On("GetAccountLedgerSequence", masterKP.Address()).
					Return(int64(1), nil).
					Once()
			},
			wantErrIs: ErrAccountAlreadyExists,
		},
		{
			name: "🔴invalid_signers_weight",
			opts: SponsorAccountCreationOptions{
				Address: masterKP.Address(),
				Signers: []entities.Signer{
					{
						Address: masterKP.Address(),
						Weight:  0,
						Type:    entities.PartialSignerType,
					},
				},
			},
			prepareMocks: func(t *testing.T, mockRPCService *RPCServiceMock) {
				mockRPCService.
					On("GetAccountLedgerSequence", masterKP.Address()).
					Return(int64(0), ErrAccountNotFound).
					Once()
			},
			wantErrContains: "validating signers weights: no full signers provided",
		},
		{
			name: "🔴sponsorship_limit_reached",
			opts: SponsorAccountCreationOptions{
				Address: masterKP.Address(),
				Signers: func() []entities.Signer {
					signers := []entities.Signer{}
					for range maxSponsoredBaseReserves - 2 {
						signers = append(signers, entities.Signer{
							Address: keypair.MustRandom().Address(),
							Weight:  10,
							Type:    entities.FullSignerType,
						})
					}
					return signers
				}(),
				Assets: []entities.Asset{
					{
						Code:   "USDC",
						Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
					},
				},
			},
			prepareMocks: func(t *testing.T, mockRPCService *RPCServiceMock) {
				mockRPCService.
					On("GetAccountLedgerSequence", masterKP.Address()).
					Return(int64(0), ErrAccountNotFound).
					Once()
			},
			wantErrIs: ErrSponsorshipLimitExceeded,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mSigClient := signing.SignatureClientMock{}
			defer mSigClient.AssertExpectations(t)
			mRPCService := RPCServiceMock{}
			defer mRPCService.AssertExpectations(t)
			accSponsorshipSvc := accountSponsorshipService{
				DistributionAccountSignatureClient: &mSigClient,
				ChannelAccountSignatureClient:      &mSigClient,
				RPCService:                         &mRPCService,
				MaxSponsoredBaseReserves:           maxSponsoredBaseReserves,
				BaseFee:                            txnbuild.MinBaseFee,
			}

			if tc.prepareMocks != nil {
				tc.prepareMocks(t, &mRPCService)
			}

			txe, err := accSponsorshipSvc.SponsorAccountCreationTransaction(ctx, tc.opts)
			if tc.wantErrIs != nil {
				assert.ErrorIs(t, tc.wantErrIs, err)
			}
			if tc.wantErrContains != "" {
				assert.EqualError(t, err, tc.wantErrContains)
			}
			assert.Empty(t, txe)
		})
	}
}

func Test_AccountSponsorshipService_SponsorAccountCreationTransaction_success(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	const maxSponsoredBaseReserves = 5
	const baseBee = 100_000
	ctx := context.Background()
	masterKP := keypair.MustRandom()
	channelAccKP := keypair.MustRandom()
	distAccKP := keypair.MustRandom()
	usdcAsset := entities.Asset{
		Code:   "USDC",
		Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
	}
	fullSignerKP := keypair.MustRandom()
	partialSigner1KP := keypair.MustRandom()
	partialSigner2KP := keypair.MustRandom()

	testCases := []struct {
		name           string
		wantOperations []txnbuild.Operation
		opts           SponsorAccountCreationOptions
	}{
		{
			name: "🟢base_case",
			opts: SponsorAccountCreationOptions{
				Address: masterKP.Address(),
			},
			wantOperations: []txnbuild.Operation{
				&txnbuild.BeginSponsoringFutureReserves{
					SponsoredID:   masterKP.Address(),
					SourceAccount: distAccKP.Address(),
				},
				&txnbuild.CreateAccount{
					Destination:   masterKP.Address(),
					Amount:        "0",
					SourceAccount: distAccKP.Address(),
				},
				&txnbuild.EndSponsoringFutureReserves{
					SourceAccount: masterKP.Address(),
				},
			},
		},
		{
			name: "🟢with_trustlines",
			opts: SponsorAccountCreationOptions{
				Address: masterKP.Address(),
				Assets:  []entities.Asset{usdcAsset},
			},
			wantOperations: []txnbuild.Operation{
				&txnbuild.BeginSponsoringFutureReserves{
					SponsoredID:   masterKP.Address(),
					SourceAccount: distAccKP.Address(),
				},
				&txnbuild.CreateAccount{
					Destination:   masterKP.Address(),
					Amount:        "0",
					SourceAccount: distAccKP.Address(),
				},
				&txnbuild.ChangeTrust{
					Line: txnbuild.CreditAsset{
						Code:   usdcAsset.Code,
						Issuer: usdcAsset.Issuer,
					}.MustToChangeTrustAsset(),
					SourceAccount: masterKP.Address(),
				},
				&txnbuild.EndSponsoringFutureReserves{
					SourceAccount: masterKP.Address(),
				},
			},
		},
		{
			name: "🟢with_signers",
			opts: SponsorAccountCreationOptions{
				Address: masterKP.Address(),
				Signers: []entities.Signer{
					{
						Address: fullSignerKP.Address(),
						Weight:  10,
						Type:    entities.FullSignerType,
					},
					{
						Address: partialSigner1KP.Address(),
						Weight:  5,
						Type:    entities.PartialSignerType,
					},
					{
						Address: partialSigner2KP.Address(),
						Weight:  5,
						Type:    entities.PartialSignerType,
					},
				},
			},
			wantOperations: []txnbuild.Operation{
				&txnbuild.BeginSponsoringFutureReserves{
					SponsoredID:   masterKP.Address(),
					SourceAccount: distAccKP.Address(),
				},
				&txnbuild.CreateAccount{
					Destination:   masterKP.Address(),
					Amount:        "0",
					SourceAccount: distAccKP.Address(),
				},
				&txnbuild.SetOptions{
					Signer: &txnbuild.Signer{
						Address: fullSignerKP.Address(),
						Weight:  10,
					},
					SourceAccount: masterKP.Address(),
				},
				&txnbuild.SetOptions{
					Signer: &txnbuild.Signer{
						Address: partialSigner1KP.Address(),
						Weight:  5,
					},
					SourceAccount: masterKP.Address(),
				},
				&txnbuild.SetOptions{
					Signer: &txnbuild.Signer{
						Address: partialSigner2KP.Address(),
						Weight:  5,
					},
					SourceAccount: masterKP.Address(),
				},
				&txnbuild.EndSponsoringFutureReserves{
					SourceAccount: masterKP.Address(),
				},
				&txnbuild.SetOptions{
					LowThreshold:    txnbuild.NewThreshold(txnbuild.Threshold(10)),
					MediumThreshold: txnbuild.NewThreshold(txnbuild.Threshold(10)),
					HighThreshold:   txnbuild.NewThreshold(txnbuild.Threshold(10)),
					SourceAccount:   masterKP.Address(),
				},
			},
		},
		{
			name: "🟢with_signer(master_weight=0)",
			opts: SponsorAccountCreationOptions{
				Address: masterKP.Address(),
				Signers: []entities.Signer{
					{
						Address: fullSignerKP.Address(),
						Weight:  10,
						Type:    entities.FullSignerType,
					},
				},
				MasterSignerWeight: utils.PointOf(0),
			},
			wantOperations: []txnbuild.Operation{
				&txnbuild.BeginSponsoringFutureReserves{
					SponsoredID:   masterKP.Address(),
					SourceAccount: distAccKP.Address(),
				},
				&txnbuild.CreateAccount{
					Destination:   masterKP.Address(),
					Amount:        "0",
					SourceAccount: distAccKP.Address(),
				},
				&txnbuild.SetOptions{
					Signer: &txnbuild.Signer{
						Address: fullSignerKP.Address(),
						Weight:  10,
					},
					SourceAccount: masterKP.Address(),
				},
				&txnbuild.EndSponsoringFutureReserves{
					SourceAccount: masterKP.Address(),
				},
				&txnbuild.SetOptions{
					MasterWeight:    txnbuild.NewThreshold(txnbuild.Threshold(0)),
					LowThreshold:    txnbuild.NewThreshold(txnbuild.Threshold(10)),
					MediumThreshold: txnbuild.NewThreshold(txnbuild.Threshold(10)),
					HighThreshold:   txnbuild.NewThreshold(txnbuild.Threshold(10)),
					SourceAccount:   masterKP.Address(),
				},
			},
		},
		{
			name: "🟢with_asset_and_signer(master_weight=0)",
			opts: SponsorAccountCreationOptions{
				Address: masterKP.Address(),
				Signers: []entities.Signer{
					{
						Address: fullSignerKP.Address(),
						Weight:  10,
						Type:    entities.FullSignerType,
					},
				},
				MasterSignerWeight: utils.PointOf(0),
				Assets:             []entities.Asset{usdcAsset},
			},
			wantOperations: []txnbuild.Operation{
				&txnbuild.BeginSponsoringFutureReserves{
					SponsoredID:   masterKP.Address(),
					SourceAccount: distAccKP.Address(),
				},
				&txnbuild.CreateAccount{
					Destination:   masterKP.Address(),
					Amount:        "0",
					SourceAccount: distAccKP.Address(),
				},
				&txnbuild.SetOptions{
					Signer: &txnbuild.Signer{
						Address: fullSignerKP.Address(),
						Weight:  10,
					},
					SourceAccount: masterKP.Address(),
				},
				&txnbuild.ChangeTrust{
					Line: txnbuild.CreditAsset{
						Code:   usdcAsset.Code,
						Issuer: usdcAsset.Issuer,
					}.MustToChangeTrustAsset(),
					SourceAccount: masterKP.Address(),
				},
				&txnbuild.EndSponsoringFutureReserves{
					SourceAccount: masterKP.Address(),
				},
				&txnbuild.SetOptions{
					MasterWeight:    txnbuild.NewThreshold(txnbuild.Threshold(0)),
					LowThreshold:    txnbuild.NewThreshold(txnbuild.Threshold(10)),
					MediumThreshold: txnbuild.NewThreshold(txnbuild.Threshold(10)),
					HighThreshold:   txnbuild.NewThreshold(txnbuild.Threshold(10)),
					SourceAccount:   masterKP.Address(),
				},
			},
		},
	}

	copyTx := func(t *testing.T, tx *txnbuild.Transaction) *txnbuild.Transaction {
		txe, err := tx.Base64()
		require.NoError(t, err)
		genericTx, err := txnbuild.TransactionFromXDR(txe)
		require.NoError(t, err)
		resultTx, ok := genericTx.Transaction()
		require.True(t, ok)

		return resultTx
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				dbConnectionPool.ExecContext(ctx, "DELETE FROM accounts")
			})

			// Mock metrics service
			mMetricsService := metrics.NewMockMetricsService()
			mMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.AnythingOfType("float64")).Once()
			mMetricsService.On("IncDBQuery", "INSERT", "accounts").Once()
			mMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.AnythingOfType("float64")).Once()
			mMetricsService.On("IncDBQuery", "SELECT", "accounts").Once()
			defer mMetricsService.AssertExpectations(t)
			models, err := data.NewModels(dbConnectionPool, mMetricsService)
			require.NoError(t, err)

			// Mock RPC service
			mRPCService := NewRPCServiceMock(t)
			mRPCService.
				On("GetAccountLedgerSequence", tc.opts.Address).
				Return(int64(0), ErrAccountNotFound).
				Once().
				On("GetAccountLedgerSequence", channelAccKP.Address()).
				Return(int64(1), nil).
				Once()
			defer mRPCService.AssertExpectations(t)

			// Mock channelAccountSignatureClient
			mChAccSigClient := signing.NewSignatureClientMock(t)
			var tx *txnbuild.Transaction
			mChAccSigClient.
				On("GetAccountPublicKey", ctx).
				Return(channelAccKP.Address(), nil).
				Once().
				On("SignStellarTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction"), []string{channelAccKP.Address()}).
				Run(func(args mock.Arguments) {
					gotTx, ok := args.Get(1).(*txnbuild.Transaction)
					require.True(t, ok)
					gotTx = copyTx(t, gotTx)

					wantTx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
						SourceAccount: &txnbuild.SimpleAccount{
							AccountID: channelAccKP.Address(),
							Sequence:  1,
						},
						IncrementSequenceNum: true,
						BaseFee:              baseBee,
						Preconditions: txnbuild.Preconditions{
							TimeBounds: gotTx.Timebounds(),
						},
						Operations: tc.wantOperations,
					})
					require.NoError(t, err)
					wantTx = copyTx(t, wantTx)
					require.Equal(t, wantTx, gotTx)
				}).
				Return(func(ctx context.Context, inTx *txnbuild.Transaction, stellarAccounts ...string) (*txnbuild.Transaction, error) {
					tx, err = inTx.Sign(network.TestNetworkPassphrase, channelAccKP) // <--- assign the signed tx to the `tx` variable
					require.NoError(t, err)
					return tx, nil
				}, nil).
				Once()

			// Mock distributionAccountSignatureClient
			mDistAccSigClient := signing.NewSignatureClientMock(t)
			mDistAccSigClient.
				On("GetAccountPublicKey", ctx).
				Return(distAccKP.Address(), nil).
				Once().
				On("SignStellarTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction"), []string{distAccKP.Address()}).
				Run(func(args mock.Arguments) {
					gotTx, ok := args.Get(1).(*txnbuild.Transaction)
					require.True(t, ok)
					require.Equal(t, tx, gotTx) // <--- this is the `tx` previously signed by the chAccSigClient
				}).
				Return(func(ctx context.Context, inTx *txnbuild.Transaction, stellarAccounts ...string) (*txnbuild.Transaction, error) {
					tx, err = inTx.Sign(network.TestNetworkPassphrase, distAccKP) // <--- assign the signed tx to the `tx` variable
					require.NoError(t, err)
					return tx, nil
				}, nil).
				Once()
			defer mChAccSigClient.AssertExpectations(t)

			// Finally, create the account sponsorship service and test it
			accSponsorshipSvc := accountSponsorshipService{
				DistributionAccountSignatureClient: mDistAccSigClient,
				ChannelAccountSignatureClient:      mChAccSigClient,
				RPCService:                         mRPCService,
				MaxSponsoredBaseReserves:           maxSponsoredBaseReserves,
				BaseFee:                            baseBee,
				Models:                             models,
			}
			gotTxXDR, err := accSponsorshipSvc.SponsorAccountCreationTransaction(ctx, tc.opts)

			// Assert tx result
			assert.NoError(t, err)
			assert.NotEmpty(t, gotTxXDR)
			wantTxXDR, err := tx.Base64()
			require.NoError(t, err)
			assert.Equal(t, wantTxXDR, gotTxXDR)

			// Assert account is fee bump eligible
			isFeeBumpEligible, err := models.Account.IsAccountFeeBumpEligible(ctx, tc.opts.Address)
			require.NoError(t, err)
			assert.True(t, isFeeBumpEligible)
		})
	}
}

func TestAccountSponsorshipServiceWrapTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
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

		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Once()
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

		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Once()
		defer mockMetricsService.AssertExpectations(t)

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
		var opNotAllowedErr *OperationNotAllowedError
		assert.ErrorAs(t, err, &opNotAllowedErr)
		assert.Empty(t, feeBumpTxe)
		assert.Empty(t, networkPassphrase)
	})

	t.Run("transaction_fee_exceeds_maximum_base_fee_for_sponsoring", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()

		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Once()
		defer mockMetricsService.AssertExpectations(t)

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

		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Once()
		defer mockMetricsService.AssertExpectations(t)

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

		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "accounts").Once()
		defer mockMetricsService.AssertExpectations(t)

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
