package services

import (
	"context"
	"net/http"
	"testing"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/support/render/problem"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestAccountSponsorshipServiceSponsorAccountCreationTransaction(t *testing.T) {
	signatureClient := signing.SignatureClientMock{}
	defer signatureClient.AssertExpectations(t)
	horizonClient := horizonclient.MockClient{}
	defer horizonClient.AssertExpectations(t)

	ctx := context.Background()
	s, err := NewAccountSponsorshipService(&signatureClient, &horizonClient, 10, txnbuild.MinBaseFee)
	require.NoError(t, err)

	t.Run("account_already_exists", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom().Address()

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: accountToSponsor,
			}).
			Return(horizon.Account{}, nil).
			Once()

		txe, networkPassphrase, err := s.SponsorAccountCreationTransaction(ctx, accountToSponsor, []entities.Signer{}, []entities.Asset{})
		assert.ErrorIs(t, ErrAccountAlreadyExists, err)
		assert.Empty(t, txe)
		assert.Empty(t, networkPassphrase)
	})

	t.Run("invalid_signers_weight", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom().Address()

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: accountToSponsor,
			}).
			Return(horizon.Account{}, horizonclient.Error{
				Response: &http.Response{},
				Problem: problem.P{
					Type: "https://stellar.org/horizon-errors/not_found",
				},
			}).
			Once()

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

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: accountToSponsor,
			}).
			Return(horizon.Account{}, horizonclient.Error{
				Response: &http.Response{},
				Problem: problem.P{
					Type: "https://stellar.org/horizon-errors/not_found",
				},
			}).
			Once()

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

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: accountToSponsor,
			}).
			Return(horizon.Account{}, horizonclient.Error{
				Response: &http.Response{},
				Problem: problem.P{
					Type: "https://stellar.org/horizon-errors/not_found",
				},
			}).
			Once().
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: distributionAccount.Address(),
			}).
			Return(horizon.Account{
				AccountID: distributionAccount.Address(),
				Sequence:  1,
			}, nil).
			Once()

		signedTx := txnbuild.Transaction{}
		signatureClient.
			On("GetDistributionAccountPublicKey").
			Return(distributionAccount.Address()).
			Times(3).
			On("NetworkPassphrase").
			Return(network.TestNetworkPassphrase).
			Once().
			On("SignStellarTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction")).
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
	})
}
