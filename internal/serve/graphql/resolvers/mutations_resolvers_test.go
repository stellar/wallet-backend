package resolvers

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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

type mockAccountSponsorshipService struct {
	mock.Mock
}

func (m *mockAccountSponsorshipService) WrapTransaction(ctx context.Context, tx *txnbuild.Transaction) (string, string, error) {
	args := m.Called(ctx, tx)
	return args.String(0), args.String(1), args.Error(2)
}

func (m *mockAccountSponsorshipService) SponsorAccountCreationTransaction(ctx context.Context, address string, signers []entities.Signer, supportedAssets []entities.Asset) (string, string, error) {
	args := m.Called(ctx, address, signers, supportedAssets)
	return args.String(0), args.String(1), args.Error(2)
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
		assert.ErrorContains(t, err, "Account is already registered")

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
		assert.ErrorContains(t, err, "Invalid address: must be a valid Stellar public key or contract address")

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
		assert.Equal(t, "Account deregistered successfully", *result.Message)

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
		assert.ErrorContains(t, err, "Account not found")

		mockService.AssertExpectations(t)
	})
}

func TestMutationResolver_CreateFeeBumpTransaction(t *testing.T) {
	ctx := context.Background()

	// Create a valid transaction XDR dynamically
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

	validTransactionXDR, err := tx.Base64()
	require.NoError(t, err)

	t.Run("success", func(t *testing.T) { // TODO
		mockAccountService := &mockAccountService{}
		mockSponsorshipService := &mockAccountSponsorshipService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:            mockAccountService,
				accountSponsorshipService: mockSponsorshipService,
				models:                    &data.Models{},
			},
		}

		input := graphql.CreateFeeBumpTransactionInput{
			Transaction: validTransactionXDR,
		}

		expectedFeeBumpXDR := "AAAABQAAAABkQfakNyGM9UOqzKXmFGHuO8Jk9VFOc+pQCOPpKhHDhAAAAZAAAAACAAAAAgAAAABkQfakNyGM9UOqzKXmFGHuO8Jk9VFOc+pQCOPpKhHDhAAAAGQAAAABAAAAAgAAAAEAAAAAAAAAAAAAAABjUwbKAAAAAAAAAAEAAAABAAAAAGRB9qQ3IYz1Q6rMpeYUYe47wmT1UU5z6lAI4+kqEcOEAAAADgAAAAVoZWxsbwAAAAAAAAEAAAAHd29ybGQhAAAAAAAAAA=="
		expectedNetworkPassphrase := "Test SDF Network ; September 2015"

		mockSponsorshipService.On("WrapTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction")).Return(expectedFeeBumpXDR, expectedNetworkPassphrase, nil)

		result, err := resolver.CreateFeeBumpTransaction(ctx, input)

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.Success)
		assert.Equal(t, expectedFeeBumpXDR, result.Transaction)
		assert.Equal(t, expectedNetworkPassphrase, result.NetworkPassphrase)

		mockSponsorshipService.AssertExpectations(t)
	})

	t.Run("invalid transaction XDR", func(t *testing.T) { // Tested
		mockAccountService := &mockAccountService{}
		mockSponsorshipService := &mockAccountSponsorshipService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:            mockAccountService,
				accountSponsorshipService: mockSponsorshipService,
				models:                    &data.Models{},
			},
		}

		input := graphql.CreateFeeBumpTransactionInput{
			Transaction: "invalid-xdr",
		}

		result, err := resolver.CreateFeeBumpTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "Could not parse transaction envelope.")
	})

	t.Run("account not eligible for sponsorship", func(t *testing.T) { // Tested
		mockAccountService := &mockAccountService{}
		mockSponsorshipService := &mockAccountSponsorshipService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:            mockAccountService,
				accountSponsorshipService: mockSponsorshipService,
				models:                    &data.Models{},
			},
		}

		input := graphql.CreateFeeBumpTransactionInput{
			Transaction: validTransactionXDR,
		}

		mockSponsorshipService.On("WrapTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction")).Return("", "", services.ErrAccountNotEligibleForBeingSponsored)

		result, err := resolver.CreateFeeBumpTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, services.ErrAccountNotEligibleForBeingSponsored.Error())

		mockSponsorshipService.AssertExpectations(t)
	})

	t.Run("fee exceeds maximum", func(t *testing.T) { // TODO
		mockAccountService := &mockAccountService{}
		mockSponsorshipService := &mockAccountSponsorshipService{}

		resolver := &mutationResolver{
			&Resolver{
				accountService:            mockAccountService,
				accountSponsorshipService: mockSponsorshipService,
				models:                    &data.Models{},
			},
		}

		input := graphql.CreateFeeBumpTransactionInput{
			Transaction: validTransactionXDR,
		}

		mockSponsorshipService.On("WrapTransaction", ctx, mock.AnythingOfType("*txnbuild.Transaction")).Return("", "", services.ErrFeeExceedsMaximumBaseFee)

		result, err := resolver.CreateFeeBumpTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, services.ErrFeeExceedsMaximumBaseFee.Error())

		mockSponsorshipService.AssertExpectations(t)
	})
}
