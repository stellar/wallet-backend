package serve

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
)

func TestValidateDistributionAccount(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		distributionAccountSignatureClient := signing.NewSignatureClientMock(t)
		rpcService := services.NewRPCServiceMock(t)

		distributionAccountSignatureClient.
			On("GetAccountPublicKey", ctx).
			Return("GABC123", nil).
			Once()
		rpcService.
			On("GetAccountLedgerSequence", "GABC123").
			Return(int64(123), nil).
			Once()

		err := validateDistributionAccount(ctx, distributionAccountSignatureClient, rpcService)
		require.NoError(t, err)
	})

	t.Run("public key lookup fails", func(t *testing.T) {
		distributionAccountSignatureClient := signing.NewSignatureClientMock(t)
		rpcService := services.NewRPCServiceMock(t)

		distributionAccountSignatureClient.
			On("GetAccountPublicKey", ctx).
			Return("", errors.New("boom")).
			Once()

		err := validateDistributionAccount(ctx, distributionAccountSignatureClient, rpcService)
		require.EqualError(t, err, "getting distribution account public key: boom")
	})

	t.Run("distribution account not found", func(t *testing.T) {
		distributionAccountSignatureClient := signing.NewSignatureClientMock(t)
		rpcService := services.NewRPCServiceMock(t)

		distributionAccountSignatureClient.
			On("GetAccountPublicKey", ctx).
			Return("GABC123", nil).
			Once()
		rpcService.
			On("GetAccountLedgerSequence", "GABC123").
			Return(int64(0), services.ErrAccountNotFound).
			Once()

		err := validateDistributionAccount(ctx, distributionAccountSignatureClient, rpcService)
		require.Error(t, err)
		assert.ErrorContains(t, err, "distribution account GABC123 does not exist on the configured Stellar network")
		assert.ErrorIs(t, err, services.ErrAccountNotFound)
	})

	t.Run("rpc failure", func(t *testing.T) {
		distributionAccountSignatureClient := signing.NewSignatureClientMock(t)
		rpcService := services.NewRPCServiceMock(t)

		distributionAccountSignatureClient.
			On("GetAccountPublicKey", ctx).
			Return("GABC123", nil).
			Once()
		rpcService.
			On("GetAccountLedgerSequence", "GABC123").
			Return(int64(0), errors.New("rpc unavailable")).
			Once()

		err := validateDistributionAccount(ctx, distributionAccountSignatureClient, rpcService)
		require.EqualError(t, err, "validating distribution account GABC123: rpc unavailable")
	})
}

func TestValidateChannelAccountsForStartup(t *testing.T) {
	ctx := context.Background()

	t.Run("startup fails when channel account validation fails", func(t *testing.T) {
		validator := &channelAccountStartupValidatorStub{
			err: errors.New("stored channel account count 0 is below required minimum 2"),
		}

		err := validateChannelAccountsForStartup(ctx, validator, 2)
		require.EqualError(t, err, "validating channel accounts: stored channel account count 0 is below required minimum 2")
		assert.Equal(t, int64(2), validator.minimum)
	})

	t.Run("startup succeeds with extra valid channel accounts", func(t *testing.T) {
		validator := &channelAccountStartupValidatorStub{}

		err := validateChannelAccountsForStartup(ctx, validator, 1)
		require.NoError(t, err)
		assert.Equal(t, int64(1), validator.minimum)
	})
}

type channelAccountStartupValidatorStub struct {
	minimum int64
	err     error
}

func (s *channelAccountStartupValidatorStub) ValidateChannelAccounts(_ context.Context, minimum int64) error {
	s.minimum = minimum
	return s.err
}
