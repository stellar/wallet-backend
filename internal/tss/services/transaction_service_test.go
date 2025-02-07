package services

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
)

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
	distributionAccountSignatureClient := signing.SignatureClientMock{}
	channelAccountSignatureClient := signing.SignatureClientMock{}
	channelAccountStore := store.ChannelAccountStoreMock{}
	mockRPCService := &services.RPCServiceMock{}
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DB:                                 dbConnectionPool,
		DistributionAccountSignatureClient: &distributionAccountSignatureClient,
		ChannelAccountSignatureClient:      &channelAccountSignatureClient,
		ChannelAccountStore:                &channelAccountStore,
		RPCService:                         mockRPCService,
		BaseFee:                            114,
	})
	atomicTxErrorPrefix := "running atomic function in RunInTransactionWithResult: "
	t.Run("channel_account_signature_client_get_account_public_key_err", func(t *testing.T) {
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return("", errors.New("channel accounts unavailable")).
			Once()

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, 30)

		channelAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.Equal(t, atomicTxErrorPrefix+"getting channel account public key: channel accounts unavailable", err.Error())
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

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, 30)

		channelAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, tx)
		expectedErr := fmt.Errorf("getting ledger sequence for channel account public key: %s: rpc service down", channelAccount.Address())
		assert.Equal(t, atomicTxErrorPrefix+expectedErr.Error(), err.Error())
	})

	t.Run("build_tx_fails", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once()

		mockRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{}, 30)

		channelAccountSignatureClient.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.Equal(t, atomicTxErrorPrefix+"building transaction: transaction has no operations", err.Error())

	})

	t.Run("lock_channel_account_to_tx_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once().
			On("NetworkPassphrase").
			Return("networkpassphrase").
			Once()

		channelAccountStore.
			On("AssignTxToChannelAccount", context.Background(), channelAccount.Address(), mock.AnythingOfType("string")).
			Return(errors.New("unable to assign channel account to tx")).
			Once()

		mockRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		payment := txnbuild.Payment{
			Destination:   keypair.MustRandom().Address(),
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: keypair.MustRandom().Address(),
		}
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{&payment}, 30)

		channelAccountSignatureClient.AssertExpectations(t)
		channelAccountStore.AssertExpectations(t)
		assert.Empty(t, tx)
		assert.Equal(t, atomicTxErrorPrefix+"assigning channel account to tx: unable to assign channel account to tx", err.Error())
	})

	t.Run("sign_stellar_transaction_w_channel_account_err", func(t *testing.T) {
		channelAccount := keypair.MustRandom()
		channelAccountSignatureClient.
			On("GetAccountPublicKey", context.Background()).
			Return(channelAccount.Address(), nil).
			Once().
			On("NetworkPassphrase").
			Return("networkpassphrase").
			Once().
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{channelAccount.Address()}).
			Return(nil, errors.New("unable to sign")).
			Once()

		channelAccountStore.
			On("AssignTxToChannelAccount", context.Background(), channelAccount.Address(), mock.AnythingOfType("string")).
			Return(nil).
			Once()

		mockRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		payment := txnbuild.Payment{
			Destination:   keypair.MustRandom().Address(),
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: keypair.MustRandom().Address(),
		}
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{&payment}, 30)

		channelAccountSignatureClient.AssertExpectations(t)
		channelAccountStore.AssertExpectations(t)
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
			On("NetworkPassphrase").
			Return("networkpassphrase").
			On("SignStellarTransaction", context.Background(), mock.AnythingOfType("*txnbuild.Transaction"), []string{channelAccount.Address()}).
			Return(signedTx, nil).
			Once()

		channelAccountStore.
			On("AssignTxToChannelAccount", context.Background(), channelAccount.Address(), mock.AnythingOfType("string")).
			Return(nil).
			Once()

		mockRPCService.
			On("GetAccountLedgerSequence", channelAccount.Address()).
			Return(int64(1), nil).
			Once()
		defer mockRPCService.AssertExpectations(t)

		payment := txnbuild.Payment{
			Destination:   keypair.MustRandom().Address(),
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: keypair.MustRandom().Address(),
		}
		tx, err := txService.BuildAndSignTransactionWithChannelAccount(context.Background(), []txnbuild.Operation{&payment}, 30)

		channelAccountSignatureClient.AssertExpectations(t)
		channelAccountStore.AssertExpectations(t)
		assert.Equal(t, signedTx, tx)
		assert.NoError(t, err)
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
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DB:                                 dbConnectionPool,
		DistributionAccountSignatureClient: &distributionAccountSignatureClient,
		ChannelAccountSignatureClient:      &channelAccountSignatureClient,
		ChannelAccountStore:                &channelAccountStore,
		RPCService:                         mockRPCService,
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
