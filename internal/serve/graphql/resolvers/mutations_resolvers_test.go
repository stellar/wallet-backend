package resolvers

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/data"
	graphql "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	"github.com/stellar/wallet-backend/internal/services"
)

type mockTransactionService struct {
	mock.Mock
}

func (m *mockTransactionService) NetworkPassphrase() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockTransactionService) BuildAndSignTransactionWithChannelAccount(ctx context.Context, genericTx *txnbuild.GenericTransaction) (*txnbuild.Transaction, error) {
	args := m.Called(ctx, genericTx)
	return args.Get(0).(*txnbuild.Transaction), args.Error(1)
}

type mockFeeBumpService struct {
	mock.Mock
}

func (m *mockFeeBumpService) WrapTransaction(ctx context.Context, tx *txnbuild.Transaction) (string, string, error) {
	args := m.Called(ctx, tx)
	return args.String(0), args.String(1), args.Error(2)
}

func (m *mockFeeBumpService) GetMaximumBaseFee() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

func TestMutationResolver_CreateFeeBumpTransaction(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mockFeeBumpService := &mockFeeBumpService{}

		resolver := &mutationResolver{
			&Resolver{
				feeBumpService: mockFeeBumpService,
				models:         &data.Models{},
			},
		}

		// Create a valid transaction
		sourceAccount := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: sourceAccount.Address()},
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

		txe, err := tx.Base64()
		require.NoError(t, err)

		input := graphql.CreateFeeBumpTransactionInput{
			TransactionXdr: txe,
		}

		expectedFeeBumpTxe := "fee-bump-envelope-xdr"
		expectedNetworkPassphrase := "Test SDF Network ; September 2015"

		mockFeeBumpService.On("WrapTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction")).Return(expectedFeeBumpTxe, expectedNetworkPassphrase, nil)

		result, err := resolver.CreateFeeBumpTransaction(ctx, input)

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.Success)
		assert.Equal(t, expectedFeeBumpTxe, result.Transaction)
		assert.Equal(t, expectedNetworkPassphrase, result.NetworkPassphrase)

		mockFeeBumpService.AssertExpectations(t)
	})

	t.Run("invalid transaction XDR", func(t *testing.T) {
		mockFeeBumpService := &mockFeeBumpService{}

		resolver := &mutationResolver{
			&Resolver{
				feeBumpService: mockFeeBumpService,
				models:         &data.Models{},
			},
		}

		input := graphql.CreateFeeBumpTransactionInput{
			TransactionXdr: "invalid-transaction-xdr",
		}

		result, err := resolver.CreateFeeBumpTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, ErrMsgCouldNotParseTransactionEnvelope)

		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			assert.Equal(t, "INVALID_TRANSACTION_XDR", gqlErr.Extensions["code"])
		}

		mockFeeBumpService.AssertExpectations(t)
	})

	t.Run("fee bump transaction rejected", func(t *testing.T) {
		mockFeeBumpService := &mockFeeBumpService{}

		resolver := &mutationResolver{
			&Resolver{
				feeBumpService: mockFeeBumpService,
				models:         &data.Models{},
			},
		}

		// Create a fee-bump transaction (which should be rejected)
		sourceAccount := keypair.MustRandom()
		distributionAccount := keypair.MustRandom()

		innerTx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: sourceAccount.Address()},
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

		feeBumpTx, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
			Inner:      innerTx,
			FeeAccount: distributionAccount.Address(),
			BaseFee:    txnbuild.MinBaseFee,
		})
		require.NoError(t, err)

		feeBumpTxe, err := feeBumpTx.Base64()
		require.NoError(t, err)

		input := graphql.CreateFeeBumpTransactionInput{
			TransactionXdr: feeBumpTxe,
		}

		result, err := resolver.CreateFeeBumpTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, ErrMsgCannotWrapFeeBumpTransaction)

		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			assert.Equal(t, "FEE_BUMP_TX_NOT_ALLOWED", gqlErr.Extensions["code"])
		}

		mockFeeBumpService.AssertExpectations(t)
	})

	t.Run("fee exceeds maximum base fee", func(t *testing.T) {
		mockFeeBumpService := &mockFeeBumpService{}

		resolver := &mutationResolver{
			&Resolver{
				feeBumpService: mockFeeBumpService,
				models:         &data.Models{},
			},
		}

		sourceAccount := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: sourceAccount.Address()},
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

		txe, err := tx.Base64()
		require.NoError(t, err)

		input := graphql.CreateFeeBumpTransactionInput{
			TransactionXdr: txe,
		}

		mockFeeBumpService.On("WrapTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction")).Return("", "", services.ErrFeeExceedsMaximumBaseFee)
		mockFeeBumpService.On("GetMaximumBaseFee").Return(int64(10000))

		result, err := resolver.CreateFeeBumpTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, services.ErrFeeExceedsMaximumBaseFee.Error())

		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			assert.Equal(t, "FEE_EXCEEDS_MAXIMUM", gqlErr.Extensions["code"])
			assert.Equal(t, int64(10000), gqlErr.Extensions["maximumBaseFee"])
		}

		mockFeeBumpService.AssertExpectations(t)
	})

	t.Run("account not eligible for sponsorship", func(t *testing.T) {
		mockFeeBumpService := &mockFeeBumpService{}

		resolver := &mutationResolver{
			&Resolver{
				feeBumpService: mockFeeBumpService,
				models:         &data.Models{},
			},
		}

		sourceAccount := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: sourceAccount.Address()},
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

		txe, err := tx.Base64()
		require.NoError(t, err)

		input := graphql.CreateFeeBumpTransactionInput{
			TransactionXdr: txe,
		}

		mockFeeBumpService.
			On("WrapTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction")).
			Return("", "", services.ErrAccountNotEligibleForBeingSponsored)

		result, err := resolver.CreateFeeBumpTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, services.ErrAccountNotEligibleForBeingSponsored.Error())

		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			assert.Equal(t, "ACCOUNT_NOT_ELIGIBLE_FOR_BEING_SPONSORED", gqlErr.Extensions["code"])
		}

		mockFeeBumpService.AssertExpectations(t)
	})

	t.Run("no signatures provided", func(t *testing.T) {
		mockFeeBumpService := &mockFeeBumpService{}

		resolver := &mutationResolver{
			&Resolver{
				feeBumpService: mockFeeBumpService,
				models:         &data.Models{},
			},
		}

		sourceAccount := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: sourceAccount.Address()},
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

		txe, err := tx.Base64()
		require.NoError(t, err)

		input := graphql.CreateFeeBumpTransactionInput{
			TransactionXdr: txe,
		}

		mockFeeBumpService.On("WrapTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction")).Return("", "", services.ErrNoSignaturesProvided)

		result, err := resolver.CreateFeeBumpTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, services.ErrNoSignaturesProvided.Error())

		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			assert.Equal(t, "NO_SIGNATURES_PROVIDED", gqlErr.Extensions["code"])
		}

		mockFeeBumpService.AssertExpectations(t)
	})

	t.Run("general service error", func(t *testing.T) {
		mockFeeBumpService := &mockFeeBumpService{}

		resolver := &mutationResolver{
			&Resolver{
				feeBumpService: mockFeeBumpService,
				models:         &data.Models{},
			},
		}

		sourceAccount := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: sourceAccount.Address()},
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

		txe, err := tx.Base64()
		require.NoError(t, err)

		input := graphql.CreateFeeBumpTransactionInput{
			TransactionXdr: txe,
		}

		mockFeeBumpService.On("WrapTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction")).Return("", "", errors.New("database connection failed"))

		result, err := resolver.CreateFeeBumpTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "Failed to create fee bump transaction: database connection failed")

		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			assert.Equal(t, "FEE_BUMP_CREATION_FAILED", gqlErr.Extensions["code"])
		}

		mockFeeBumpService.AssertExpectations(t)
	})
}

func TestMutationResolver_BuildTransaction(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				transactionService: mockTransactionService,
				models:             &data.Models{},
			},
		}

		// Create a complete test transaction
		sourceAccount := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: sourceAccount.Address()},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Asset:       txnbuild.NativeAsset{},
					Amount:      "10",
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		// Get the transaction XDR
		txXDR, err := tx.Base64()
		require.NoError(t, err)

		input := graphql.BuildTransactionInput{
			TransactionXdr: txXDR,
		}

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount", ctx, mock.AnythingOfType("*txnbuild.GenericTransaction")).Return(tx, nil)

		result, err := resolver.BuildTransaction(ctx, input)

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.Success)
		assert.NotEmpty(t, result.TransactionXdr)

		mockTransactionService.AssertExpectations(t)
	})

	t.Run("invalid transaction XDR", func(t *testing.T) {
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				transactionService: mockTransactionService,
				models:             &data.Models{},
			},
		}

		input := graphql.BuildTransactionInput{
			TransactionXdr: "invalid-xdr",
		}

		result, err := resolver.BuildTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, ErrMsgCouldNotParseTransactionEnvelope)
	})

	t.Run("transaction service error", func(t *testing.T) {
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				transactionService: mockTransactionService,
				models:             &data.Models{},
			},
		}

		// Create a complete test transaction
		sourceAccount := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: sourceAccount.Address()},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Asset:       txnbuild.NativeAsset{},
					Amount:      "10",
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		// Get the transaction XDR
		txXDR, err := tx.Base64()
		require.NoError(t, err)

		input := graphql.BuildTransactionInput{
			TransactionXdr: txXDR,
		}

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount", ctx, mock.AnythingOfType("*txnbuild.GenericTransaction")).Return((*txnbuild.Transaction)(nil), errors.New("transaction build failed"))

		result, err := resolver.BuildTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "transaction build failed")

		mockTransactionService.AssertExpectations(t)
	})

	t.Run("invalid operation structure error", func(t *testing.T) {
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				transactionService: mockTransactionService,
				models:             &data.Models{},
			},
		}

		// Create a complete test transaction
		sourceAccount := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: sourceAccount.Address()},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Asset:       txnbuild.NativeAsset{},
					Amount:      "10",
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		// Get the transaction XDR
		txXDR, err := tx.Base64()
		require.NoError(t, err)

		input := graphql.BuildTransactionInput{
			TransactionXdr: txXDR,
		}

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount", ctx, mock.AnythingOfType("*txnbuild.GenericTransaction")).Return((*txnbuild.Transaction)(nil), services.ErrInvalidTimeout)

		result, err := resolver.BuildTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "invalid timeout: timeout cannot be greater than maximum allowed seconds")

		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			assert.Equal(t, "INVALID_OPERATION_STRUCTURE", gqlErr.Extensions["code"])
		}

		mockTransactionService.AssertExpectations(t)
	})

	t.Run("invalid soroban transaction error", func(t *testing.T) {
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				transactionService: mockTransactionService,
				models:             &data.Models{},
			},
		}

		// Create a complete test transaction
		sourceAccount := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: sourceAccount.Address()},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Asset:       txnbuild.NativeAsset{},
					Amount:      "10",
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		// Get the transaction XDR
		txXDR, err := tx.Base64()
		require.NoError(t, err)

		input := graphql.BuildTransactionInput{
			TransactionXdr: txXDR,
		}

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount", ctx, mock.AnythingOfType("*txnbuild.GenericTransaction")).Return((*txnbuild.Transaction)(nil), services.ErrInvalidSorobanOperationCount)

		result, err := resolver.BuildTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "invalid Soroban transaction: must have exactly one operation")

		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			assert.Equal(t, "INVALID_SOROBAN_TRANSACTION", gqlErr.Extensions["code"])
		}

		mockTransactionService.AssertExpectations(t)
	})
}
