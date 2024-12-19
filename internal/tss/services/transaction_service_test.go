package services

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestValidateOptions(t *testing.T) {
	t.Run("return_error_when_distribution_signature_client_nil", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: nil,
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			HorizonClient:                      &horizonclient.MockClient{},
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "distribution account signature client cannot be nil", err.Error())

	})

	t.Run("return_error_when_channel_signature_client_nil", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      nil,
			HorizonClient:                      &horizonclient.MockClient{},
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "channel account signature client cannot be nil", err.Error())
	})

	t.Run("return_error_when_horizon_client_nil", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			HorizonClient:                      nil,
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "horizon client cannot be nil", err.Error())
	})

	t.Run("return_error_when_base_fee_too_low", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			HorizonClient:                      &horizonclient.MockClient{},
			BaseFee:                            txnbuild.MinBaseFee - 10,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "base fee is lower than the minimum network fee", err.Error())
	})
}

func TestBuildAndSignTransactionWithChannelAccount(t *testing.T) {
	distributionAccountSignatureClient := signing.SignatureClientMock{}
	channelAccountSignatureClient := signing.SignatureClientMock{}
	horizonClient := horizonclient.MockClient{}
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DistributionAccountSignatureClient: &distributionAccountSignatureClient,
		ChannelAccountSignatureClient:      &channelAccountSignatureClient,
		HorizonClient:                      &horizonClient,
		BaseFee:                            114,
	})

	t.Run("channel_account_signature_client_get_account_public_key_err", func(t *testing.T) {
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return("", errors.New("channel accounts unavailable")).
			Once()

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, 30)

		channelAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.Equal(t, "getting channel account public key: channel accounts unavailable", err.Error())
	})

	t.Run("horizon_client_get_account_detail_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once()

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: channelAccount.Address(),
			}).
			Return(horizon.Account{}, errors.New("horizon down")).
			Once()

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, 30)

		channelAccountSignatureClient.AssertExpectations(t)
		horizonClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.Equal(t, "getting channel account details from horizon: horizon down", err.Error())
	})

	t.Run("build_tx_fails", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once()

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: channelAccount.Address(),
			}).
			Return(horizon.Account{AccountID: channelAccount.Address(), Sequence: 1}, nil).
			Once()

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, 30)

		channelAccountSignatureClient.AssertExpectations(t)
		horizonClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.Equal(t, "building transaction: transaction has no operations", err.Error())

	})

	t.Run("sign_stellar_transaction_w_channel_account_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{channelAccount.Address()}).
			Return(nil, errors.New("unable to sign")).
			Once()

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: channelAccount.Address(),
			}).
			Return(horizon.Account{AccountID: channelAccount.Address(), Sequence: 1}, nil).
			Once()

		payment := txnbuild.Payment{
			Destination:   keypair.MustRandom().Address(),
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: keypair.MustRandom().Address(),
		}
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{&payment}, 30)

		channelAccountSignatureClient.AssertExpectations(t)
		horizonClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.Equal(t, "signing transaction with channel account: unable to sign", err.Error())
	})

	t.Run("returns_signed_tx", func(t *testing.T) {
		signedTx := utils.BuildTestTransaction()
		channelAccount := keypair.MustRandom()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{channelAccount.Address()}).
			Return(signedTx, nil).
			Once()

		horizonClient.
			On("AccountDetail", horizonclient.AccountRequest{
				AccountID: channelAccount.Address(),
			}).
			Return(horizon.Account{AccountID: channelAccount.Address(), Sequence: 1}, nil).
			Once()

		payment := txnbuild.Payment{
			Destination:   keypair.MustRandom().Address(),
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: keypair.MustRandom().Address(),
		}
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{&payment}, 30)

		channelAccountSignatureClient.AssertExpectations(t)
		horizonClient.AssertExpectations(t)
		assert.Equal(t, signedTx, tx)
		assert.NoError(t, err)
	})
}

func TestBuildFeeBumpTransaction(t *testing.T) {
	distributionAccountSignatureClient := signing.SignatureClientMock{}
	channelAccountSignatureClient := signing.SignatureClientMock{}
	horizonClient := horizonclient.MockClient{}
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DistributionAccountSignatureClient: &distributionAccountSignatureClient,
		ChannelAccountSignatureClient:      &channelAccountSignatureClient,
		HorizonClient:                      &horizonClient,
		BaseFee:                            114,
	})

	t.Run("distribution_account_signature_client_get_account_public_key_err", func(t *testing.T) {
		tx := utils.BuildTestTransaction()
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
		tx := utils.BuildTestTransaction()
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
		tx := utils.BuildTestTransaction()
		feeBump := utils.BuildTestFeeBumpTransaction()
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
