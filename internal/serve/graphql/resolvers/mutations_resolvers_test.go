package resolvers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
	graphql "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	"github.com/stellar/wallet-backend/internal/services"
)

type mockAccountService struct {
	mock.Mock
}

func (m *mockAccountService) RegisterAccount(ctx context.Context, address string) error {
	args := m.Called(ctx, address)
	return args.Error(0)
}

func (m *mockAccountService) DeregisterAccount(ctx context.Context, address string) error {
	args := m.Called(ctx, address)
	return args.Error(0)
}

type mockTransactionService struct {
	mock.Mock
}

func (m *mockTransactionService) NetworkPassphrase() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockTransactionService) BuildAndSignTransactionWithChannelAccount(ctx context.Context, genericTx *txnbuild.GenericTransaction, simulationResult *entities.RPCSimulateTransactionResult) (*txnbuild.Transaction, error) {
	args := m.Called(ctx, genericTx, simulationResult)
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

func TestMutationResolver_RegisterAccount(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mockService := &mockAccountService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService: mockService,
				models:         &data.Models{},
			},
		}

		input := graphql.RegisterAccountInput{
			Address: "GXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
		}

		mockService.On("RegisterAccount", ctx, input.Address).Return(nil)

		result, err := resolver.RegisterAccount(ctx, input)

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.Success)
		assert.NotNil(t, result.Account)
		assert.Equal(t, input.Address, result.Account.StellarAddress)

		mockService.AssertExpectations(t)
	})

	t.Run("registration fails", func(t *testing.T) {
		mockService := &mockAccountService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService: mockService,
				models:         &data.Models{}, // empty models for error case
			},
		}

		input := graphql.RegisterAccountInput{
			Address: "GXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
		}

		mockService.On("RegisterAccount", ctx, input.Address).Return(errors.New("registration failed"))

		result, err := resolver.RegisterAccount(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "Failed to register account: registration failed")

		mockService.AssertExpectations(t)
	})

	t.Run("duplicate registration fails", func(t *testing.T) {
		mockService := &mockAccountService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService: mockService,
				models:         &data.Models{},
			},
		}

		input := graphql.RegisterAccountInput{
			Address: "GXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
		}

		mockService.On("RegisterAccount", ctx, input.Address).Return(data.ErrAccountAlreadyExists)

		result, err := resolver.RegisterAccount(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, ErrMsgAccountAlreadyExists)

		mockService.AssertExpectations(t)
	})

	t.Run("empty address", func(t *testing.T) {
		mockService := &mockAccountService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService: mockService,
				models:         &data.Models{},
			},
		}

		input := graphql.RegisterAccountInput{
			Address: "",
		}

		mockService.On("RegisterAccount", ctx, "").Return(errors.New("invalid address"))

		result, err := resolver.RegisterAccount(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "Failed to register account: invalid address")

		mockService.AssertExpectations(t)
	})

	t.Run("invalid address format", func(t *testing.T) {
		mockService := &mockAccountService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService: mockService,
				models:         &data.Models{},
			},
		}

		input := graphql.RegisterAccountInput{
			Address: "invalid-stellar-address",
		}

		mockService.On("RegisterAccount", ctx, input.Address).Return(services.ErrInvalidAddress)

		result, err := resolver.RegisterAccount(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, ErrMsgInvalidAddress)

		mockService.AssertExpectations(t)
	})
}

func TestMutationResolver_DeregisterAccount(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mockService := &mockAccountService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService: mockService,
				models:         &data.Models{},
			},
		}

		input := graphql.DeregisterAccountInput{
			Address: "GXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
		}

		mockService.On("DeregisterAccount", ctx, input.Address).Return(nil)

		result, err := resolver.DeregisterAccount(ctx, input)

		require.NoError(t, err)
		assert.True(t, result.Success)
		assert.Equal(t, ErrMsgAccountDeregisteredSuccess, *result.Message)

		mockService.AssertExpectations(t)
	})

	t.Run("deregistration fails", func(t *testing.T) {
		mockService := &mockAccountService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService: mockService,
				models:         &data.Models{},
			},
		}

		input := graphql.DeregisterAccountInput{
			Address: "GXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
		}

		mockService.On("DeregisterAccount", ctx, input.Address).Return(errors.New("deregistration failed"))

		result, err := resolver.DeregisterAccount(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "Failed to deregister account: deregistration failed")

		mockService.AssertExpectations(t)
	})

	t.Run("empty address", func(t *testing.T) {
		mockService := &mockAccountService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService: mockService,
				models:         &data.Models{},
			},
		}

		input := graphql.DeregisterAccountInput{
			Address: "",
		}

		mockService.On("DeregisterAccount", ctx, "").Return(errors.New("invalid address"))

		result, err := resolver.DeregisterAccount(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "Failed to deregister account: invalid address")

		mockService.AssertExpectations(t)
	})

	t.Run("account not found", func(t *testing.T) {
		mockService := &mockAccountService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService: mockService,
				models:         &data.Models{},
			},
		}

		input := graphql.DeregisterAccountInput{
			Address: "GNONEXISTENTACCOUNTXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
		}

		mockService.On("DeregisterAccount", ctx, input.Address).Return(data.ErrAccountNotFound)

		result, err := resolver.DeregisterAccount(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, ErrMsgAccountNotFound)

		mockService.AssertExpectations(t)
	})
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
		mockAccountService := &mockAccountService{}
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:     mockAccountService,
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

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount", ctx, mock.AnythingOfType("*txnbuild.GenericTransaction"), (*entities.RPCSimulateTransactionResult)(nil)).Return(tx, nil)

		result, err := resolver.BuildTransaction(ctx, input)

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.Success)
		assert.NotEmpty(t, result.TransactionXdr)

		mockTransactionService.AssertExpectations(t)
	})

	t.Run("invalid transaction XDR", func(t *testing.T) {
		mockAccountService := &mockAccountService{}
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:     mockAccountService,
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
		mockAccountService := &mockAccountService{}
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:     mockAccountService,
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

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount", ctx, mock.AnythingOfType("*txnbuild.GenericTransaction"), (*entities.RPCSimulateTransactionResult)(nil)).Return((*txnbuild.Transaction)(nil), errors.New("transaction build failed"))

		result, err := resolver.BuildTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "transaction build failed")

		mockTransactionService.AssertExpectations(t)
	})

	t.Run("with simulation result", func(t *testing.T) {
		mockAccountService := &mockAccountService{}
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:     mockAccountService,
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

		// Create simulation result
		latestLedger := int32(12345)
		minResourceFee := "1000000"
		errorMsg := "simulation error example"
		events := []string{"event1", "event2"}

		transactionData := (*string)(nil)

		input := graphql.BuildTransactionInput{
			TransactionXdr: txXDR,
			SimulationResult: &graphql.SimulationResultInput{
				LatestLedger:    &latestLedger,
				MinResourceFee:  &minResourceFee,
				Error:           &errorMsg,
				Events:          events,
				TransactionData: transactionData,
			},
		}

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount",
			ctx,
			mock.AnythingOfType("*txnbuild.GenericTransaction"),
			mock.MatchedBy(func(simResult *entities.RPCSimulateTransactionResult) bool {
				// Verify all simulation result fields are properly converted
				return simResult != nil &&
					simResult.LatestLedger == int64(latestLedger) &&
					simResult.MinResourceFee == minResourceFee &&
					simResult.Error == errorMsg &&
					len(simResult.Events) == 2 &&
					simResult.Events[0] == "event1" &&
					simResult.Events[1] == "event2"
			})).Return(tx, nil)

		result, err := resolver.BuildTransaction(ctx, input)

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.Success)
		assert.NotEmpty(t, result.TransactionXdr)

		mockTransactionService.AssertExpectations(t)
	})

	t.Run("with valid transaction data", func(t *testing.T) {
		mockAccountService := &mockAccountService{}
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:     mockAccountService,
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

		// Create a valid SorobanTransactionData
		validTxData := xdr.SorobanTransactionData{
			Ext: xdr.SorobanTransactionDataExt{
				V: 0, // Version 0
			},
			Resources: xdr.SorobanResources{
				Footprint: xdr.LedgerFootprint{
					ReadOnly:  []xdr.LedgerKey{},
					ReadWrite: []xdr.LedgerKey{},
				},
				Instructions:  1000000,
				DiskReadBytes: 1000,
				WriteBytes:    1000,
			},
			ResourceFee: 1000000,
		}

		validTxDataBase64, err := xdr.MarshalBase64(validTxData)
		require.NoError(t, err)

		input := graphql.BuildTransactionInput{
			TransactionXdr: txXDR,
			SimulationResult: &graphql.SimulationResultInput{
				TransactionData: &validTxDataBase64,
				Events:          []string{"test-event"},
			},
		}

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount",
			ctx,
			mock.AnythingOfType("*txnbuild.GenericTransaction"),
			mock.MatchedBy(func(simResult *entities.RPCSimulateTransactionResult) bool {
				// Verify that TransactionData was successfully parsed and matches our original data
				expectedTxDataBase64, marshalErr := xdr.MarshalBase64(simResult.TransactionData)
				if marshalErr != nil {
					return false
				}
				return simResult != nil &&
					expectedTxDataBase64 == validTxDataBase64 &&
					len(simResult.Events) == 1 &&
					simResult.Events[0] == "test-event"
			})).Return(tx, nil)

		result, err := resolver.BuildTransaction(ctx, input)

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.Success)
		assert.NotEmpty(t, result.TransactionXdr)

		mockTransactionService.AssertExpectations(t)
	})

	t.Run("invalid transaction data", func(t *testing.T) {
		mockAccountService := &mockAccountService{}
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:     mockAccountService,
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

		invalidTransactionData := "invalid-transaction-data-xdr"

		input := graphql.BuildTransactionInput{
			TransactionXdr: txXDR,
			SimulationResult: &graphql.SimulationResultInput{
				TransactionData: &invalidTransactionData,
			},
		}

		result, err := resolver.BuildTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "unmarshalling transaction data")

		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			assert.Equal(t, "INVALID_SIMULATION_RESULT", gqlErr.Extensions["code"])
		}
	})

	t.Run("invalid operation structure error", func(t *testing.T) {
		mockAccountService := &mockAccountService{}
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:     mockAccountService,
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

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount", ctx, mock.AnythingOfType("*txnbuild.GenericTransaction"), (*entities.RPCSimulateTransactionResult)(nil)).Return((*txnbuild.Transaction)(nil), services.ErrInvalidTimeout)

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
		mockAccountService := &mockAccountService{}
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:     mockAccountService,
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

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount", ctx, mock.AnythingOfType("*txnbuild.GenericTransaction"), (*entities.RPCSimulateTransactionResult)(nil)).Return((*txnbuild.Transaction)(nil), services.ErrInvalidSorobanOperationCount)

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

	t.Run("invalid soroban simulation results empty error", func(t *testing.T) {
		mockAccountService := &mockAccountService{}
		mockTransactionService := &mockTransactionService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:     mockAccountService,
				transactionService: mockTransactionService,
				models:             &data.Models{},
			},
		}

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

		txXDR, err := tx.Base64()
		require.NoError(t, err)

		input := graphql.BuildTransactionInput{
			TransactionXdr: txXDR,
		}

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount", ctx, mock.AnythingOfType("*txnbuild.GenericTransaction"), (*entities.RPCSimulateTransactionResult)(nil)).Return((*txnbuild.Transaction)(nil), services.ErrInvalidSorobanSimulationResultsEmpty)

		result, err := resolver.BuildTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "invalid Soroban transaction: simulation results cannot be empty for InvokeHostFunction")

		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			assert.Equal(t, "INVALID_SOROBAN_TRANSACTION", gqlErr.Extensions["code"])
		}

		mockTransactionService.AssertExpectations(t)
	})
}

func Test_convertSimulationResult(t *testing.T) {
	t.Run("results_properly_converted", func(t *testing.T) {
		// Use a pre-built valid JSON result string with base64 XDR values.
		// The auth entry is a SourceAccount credential with a contract function invocation.
		// The xdr value is a ScVal boolean (true).
		resultStr := buildValidSimulationResultJSON(t)

		input := &graphql.SimulationResultInput{
			Results: []string{resultStr},
		}

		result, err := convertSimulationResult(input)

		require.NoError(t, err)
		require.Len(t, result.Results, 1)
		require.Len(t, result.Results[0].Auth, 1)
		assert.Equal(t, xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount, result.Results[0].Auth[0].Credentials.Type)
		assert.Equal(t, xdr.ScValTypeScvBool, result.Results[0].XDR.Type)
		assert.True(t, *result.Results[0].XDR.B)
	})

	t.Run("nil_results_returns_nil", func(t *testing.T) {
		input := &graphql.SimulationResultInput{
			Results: nil,
		}

		result, err := convertSimulationResult(input)

		require.NoError(t, err)
		assert.Nil(t, result.Results)
	})

	t.Run("empty_results_returns_nil", func(t *testing.T) {
		input := &graphql.SimulationResultInput{
			Results: []string{},
		}

		result, err := convertSimulationResult(input)

		require.NoError(t, err)
		assert.Nil(t, result.Results)
	})

	t.Run("invalid_json_returns_error", func(t *testing.T) {
		input := &graphql.SimulationResultInput{
			Results: []string{"not-json"},
		}

		_, err := convertSimulationResult(input)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshalling simulation result at index 0")
	})

	t.Run("invalid_base64_in_auth_returns_error", func(t *testing.T) {
		input := &graphql.SimulationResultInput{
			Results: []string{`{"auth":["bad-base64"],"xdr":"bad-base64"}`},
		}

		_, err := convertSimulationResult(input)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshalling simulation result at index 0")
	})
}

// buildValidSimulationResultJSON builds a valid JSON string representing an RPCSimulateHostFunctionResult.
// This mirrors what the Soroban RPC server returns and what a client forwards to the buildTransaction mutation.
func buildValidSimulationResultJSON(t *testing.T) string {
	t.Helper()

	// Build a valid SourceAccount auth entry with a contract function invocation
	signerAccountID, err := xdr.AddressToAccountId(keypair.MustRandom().Address())
	require.NoError(t, err)

	authEntry := xdr.SorobanAuthorizationEntry{
		Credentials: xdr.SorobanCredentials{
			Type: xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount,
		},
		RootInvocation: xdr.SorobanAuthorizedInvocation{
			Function: xdr.SorobanAuthorizedFunction{
				Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn,
				ContractFn: &xdr.InvokeContractArgs{
					ContractAddress: xdr.ScAddress{
						Type:      xdr.ScAddressTypeScAddressTypeAccount,
						AccountId: &signerAccountID,
					},
					FunctionName: "test",
				},
			},
		},
	}
	scVal := xdr.ScVal{Type: xdr.ScValTypeScvBool, B: boolPtr(true)}
	// Use entities.RPCSimulateHostFunctionResult so we rely on its custom JSON
	// marshalling logic rather than duplicating the wire-format encoding here.
	simResult := entities.RPCSimulateHostFunctionResult{
		Auth: []xdr.SorobanAuthorizationEntry{authEntry},
		XDR:  scVal,
	}
	resultJSON, err := json.Marshal(simResult)
	require.NoError(t, err)
	return string(resultJSON)
}

// boolPtr returns a pointer to a bool value.
func boolPtr(b bool) *bool {
	return &b
}
