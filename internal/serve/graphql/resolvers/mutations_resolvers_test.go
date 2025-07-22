package resolvers

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	graphql "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
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

func TestMutationResolver_RegisterAccount(t *testing.T) {
	ctx := context.Background()

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

		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, "Failed to register account: registration failed", *result.Message)
		assert.Nil(t, result.Account)

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

		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, "Failed to register account: invalid address", *result.Message)
		assert.Nil(t, result.Account)

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

		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, "Failed to deregister account: deregistration failed", *result.Message)

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

		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, "Failed to deregister account: invalid address", *result.Message)

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

		mockService.On("DeregisterAccount", ctx, input.Address).Return(errors.New("account not found"))

		result, err := resolver.DeregisterAccount(ctx, input)

		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, "Failed to deregister account: account not found", *result.Message)

		mockService.AssertExpectations(t)
	})
}
