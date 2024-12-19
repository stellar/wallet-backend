package services

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	tsserror "github.com/stellar/wallet-backend/internal/tss/errors"
	"github.com/stellar/wallet-backend/internal/tss/utils"
)

func TestValidateOptions(t *testing.T) {
	t.Run("return_error_when_distribution_signature_client_nil", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: nil,
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			RPCService:                         &services.RPCServiceMock{},
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "distribution account signature client cannot be nil", err.Error())

	})

	t.Run("return_error_when_channel_signature_client_nil", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      nil,
			RPCService:                         &services.RPCServiceMock{},
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "channel account signature client cannot be nil", err.Error())
	})

	t.Run("return_error_when_rpc_client_nil", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			RPCService:                         nil,
			BaseFee:                            114,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "rpc client cannot be nil", err.Error())
	})

	t.Run("return_error_when_base_fee_too_low", func(t *testing.T) {
		opts := TransactionServiceOptions{
			DistributionAccountSignatureClient: &signing.SignatureClientMock{},
			ChannelAccountSignatureClient:      &signing.SignatureClientMock{},
			RPCService:                         &services.RPCServiceMock{},
			BaseFee:                            txnbuild.MinBaseFee - 10,
		}
		err := opts.ValidateOptions()
		assert.Equal(t, "base fee is lower than the minimum network fee", err.Error())
	})
}

func TestBuildPayments(t *testing.T) {
	dest := "ABCD"
	operations := []txnbuild.Operation{
		&txnbuild.Payment{
			Destination: dest,
			Amount:      "1.0",
			Asset:       txnbuild.NativeAsset{},
		},
	}
	src := "EFGH"
	payments, error := buildPayments(src, operations)
	assert.Empty(t, error)
	assert.Equal(t, src, payments[0].(*txnbuild.Payment).SourceAccount)
	assert.Equal(t, dest, payments[0].(*txnbuild.Payment).Destination)
	assert.Equal(t, txnbuild.NativeAsset{}, payments[0].(*txnbuild.Payment).Asset)
}

func TestSignAndBuildNewFeeBumpTransaction(t *testing.T) {
	distributionAccountSignatureClient := signing.SignatureClientMock{}
	defer distributionAccountSignatureClient.AssertExpectations(t)
	channelAccountSignatureClient := signing.SignatureClientMock{}
	defer channelAccountSignatureClient.AssertExpectations(t)
	mockRPCService := &services.RPCServiceMock{}
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DistributionAccountSignatureClient: &distributionAccountSignatureClient,
		ChannelAccountSignatureClient:      &channelAccountSignatureClient,
		RPCService:                         mockRPCService,
		BaseFee:                            114,
	})

	txStr, _ := utils.BuildTestTransaction().Base64()

	t.Run("malformed_transaction_string", func(t *testing.T) {
		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), "abcd")
		assert.Empty(t, feeBumpTx)
		assert.ErrorIs(t, tsserror.OriginalXDRMalformed, err)
	})

	t.Run("channel_account_signature_client_get_account_public_key_err", func(t *testing.T) {
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return("", errors.New("channel accounts unavailable")).
			Once()

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "getting channel account public key: channel accounts unavailable", err.Error())
	})

	t.Run("rpc_client_get_account_seq_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once()

		mockRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(0), errors.New("rpc service down")).
			Once()
		defer mockRPCService.AssertExpectations(t)

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "getting channel account ledger sequence: rpc service down", err.Error())
	})

	t.Run("distribution_account_signature_client_get_account_public_key_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once()

		distributionAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return("", errors.New("client down")).
			Once()

		mockRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "getting distribution account public key: client down", err.Error())
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
		distributionAccount := keypair.MustRandom()
		distributionAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(distributionAccount.Address(), nil).
			Once()

		mockRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "signing transaction with channel account: unable to sign", err.Error())
	})

	t.Run("sign_stellar_transaction_w_distribition_account_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		signedTx := utils.BuildTestTransaction()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{channelAccount.Address()}).
			Return(signedTx, nil).
			Once()

		distributionAccount := keypair.MustRandom()
		distributionAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(distributionAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
			Return(nil, errors.New("unable to sign")).
			Once()

		mockRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "signing transaction with distribution account: unable to sign", err.Error())
	})

	t.Run("sign_feebump_transaction_w_distribition_account_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		signedTx := utils.BuildTestTransaction()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{channelAccount.Address()}).
			Return(signedTx, nil).
			Once()

		distributionAccount := keypair.MustRandom()
		distributionAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(distributionAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
			Return(signedTx, nil).
			On("SignStellarFeeBumpTransaction", context.Background(), mock.AnythingOfType("*txnbuild.FeeBumpTransaction")).
			Return(nil, errors.New("unable to sign")).
			Once()

		mockRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Empty(t, feeBumpTx)
		assert.Equal(t, "signing the fee bump transaction with distribution account: unable to sign", err.Error())
	})

	t.Run("returns_signed_tx", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		signedTx := utils.BuildTestTransaction()
		testFeeBumpTx, _ := txnbuild.NewFeeBumpTransaction(
			txnbuild.FeeBumpTransactionParams{
				Inner:      signedTx,
				FeeAccount: channelAccount.Address(),
				BaseFee:    int64(100),
			},
		)
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{channelAccount.Address()}).
			Return(signedTx, nil).
			Once()

		distributionAccount := keypair.MustRandom()
		distributionAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(distributionAccount.Address(), nil).
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{distributionAccount.Address()}).
			Return(signedTx, nil).
			On("SignStellarFeeBumpTransaction", context.Background(), mock.AnythingOfType("*txnbuild.FeeBumpTransaction")).
			Return(testFeeBumpTx, nil).
			Once()

		mockRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(context.Background(), txStr)
		assert.Equal(t, feeBumpTx, testFeeBumpTx)
		assert.Empty(t, err)
	})
}
