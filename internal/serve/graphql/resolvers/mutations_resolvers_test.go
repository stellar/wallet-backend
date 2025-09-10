package resolvers

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	xdr3 "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
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

func (m *mockTransactionService) BuildAndSignTransactionWithChannelAccount(ctx context.Context, operations []txnbuild.Operation, timeoutInSecs int64, simulationResult entities.RPCSimulateTransactionResult) (*txnbuild.Transaction, error) {
	args := m.Called(ctx, operations, timeoutInSecs, simulationResult)
	return args.Get(0).(*txnbuild.Transaction), args.Error(1)
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

		// Create a test transaction
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

		// Create a valid operation XDR
		srcAccount := keypair.MustRandom().Address()
		p := txnbuild.Payment{
			Destination:   keypair.MustRandom().Address(),
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: srcAccount,
		}
		op, err := p.BuildXDR()
		require.NoError(t, err)

		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		err = op.EncodeTo(enc)
		require.NoError(t, err)

		opXDR := buf.String()
		operationXDR := base64.StdEncoding.EncodeToString([]byte(opXDR))

		input := graphql.BuildTransactionInput{
			Transaction: &graphql.TransactionInput{
				Operations: []string{operationXDR},
				Timeout:    30,
			},
		}

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount", ctx, mock.AnythingOfType("[]txnbuild.Operation"), int64(30), mock.AnythingOfType("entities.RPCSimulateTransactionResult")).Return(tx, nil)

		result, err := resolver.BuildTransaction(ctx, input)

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.Success)
		assert.NotEmpty(t, result.TransactionXdr)

		mockTransactionService.AssertExpectations(t)
	})

	t.Run("invalid operations", func(t *testing.T) {
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
			Transaction: &graphql.TransactionInput{
				Operations: []string{"invalid-xdr"},
				Timeout:    30,
			},
		}

		result, err := resolver.BuildTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "Invalid operations")
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

		// Create a valid operation XDR
		srcAccount := keypair.MustRandom().Address()
		p := txnbuild.Payment{
			Destination:   keypair.MustRandom().Address(),
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: srcAccount,
		}
		op, err := p.BuildXDR()
		require.NoError(t, err)

		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		err = op.EncodeTo(enc)
		require.NoError(t, err)

		opXDR := buf.String()
		operationXDR := base64.StdEncoding.EncodeToString([]byte(opXDR))

		input := graphql.BuildTransactionInput{
			Transaction: &graphql.TransactionInput{
				Operations: []string{operationXDR},
				Timeout:    30,
			},
		}

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount", ctx, mock.AnythingOfType("[]txnbuild.Operation"), int64(30), mock.AnythingOfType("entities.RPCSimulateTransactionResult")).Return((*txnbuild.Transaction)(nil), errors.New("transaction build failed"))

		result, err := resolver.BuildTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "Failed to build transaction")

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

		// Create a test transaction
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

		// Create a valid operation XDR
		srcAccount := keypair.MustRandom().Address()
		p := txnbuild.Payment{
			Destination:   keypair.MustRandom().Address(),
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: srcAccount,
		}
		op, err := p.BuildXDR()
		require.NoError(t, err)

		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		err = op.EncodeTo(enc)
		require.NoError(t, err)

		opXDR := buf.String()
		operationXDR := base64.StdEncoding.EncodeToString([]byte(opXDR))

		// Create simulation result
		latestLedger := int32(12345)
		minResourceFee := "1000000"
		errorMsg := "simulation error example"
		events := []string{"event1", "event2"}

		transactionData := (*string)(nil)

		input := graphql.BuildTransactionInput{
			Transaction: &graphql.TransactionInput{
				Operations: []string{operationXDR},
				Timeout:    45,
				SimulationResult: &graphql.SimulationResultInput{
					LatestLedger:    &latestLedger,
					MinResourceFee:  &minResourceFee,
					Error:           &errorMsg,
					Events:          events,
					TransactionData: transactionData,
				},
			},
		}

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount",
			ctx,
			mock.AnythingOfType("[]txnbuild.Operation"),
			int64(45),
			mock.MatchedBy(func(simResult entities.RPCSimulateTransactionResult) bool {
				// Verify all simulation result fields are properly converted
				return simResult.LatestLedger == int64(latestLedger) &&
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

		// Create a valid operation XDR
		srcAccount := keypair.MustRandom().Address()
		p := txnbuild.Payment{
			Destination:   keypair.MustRandom().Address(),
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: srcAccount,
		}
		op, err := p.BuildXDR()
		require.NoError(t, err)

		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		err = op.EncodeTo(enc)
		require.NoError(t, err)

		opXDR := buf.String()
		operationXDR := base64.StdEncoding.EncodeToString([]byte(opXDR))

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
			Transaction: &graphql.TransactionInput{
				Operations: []string{operationXDR},
				Timeout:    30,
				SimulationResult: &graphql.SimulationResultInput{
					TransactionData: &validTxDataBase64,
					Events:          []string{"test-event"},
				},
			},
		}

		mockTransactionService.On("BuildAndSignTransactionWithChannelAccount",
			ctx,
			mock.AnythingOfType("[]txnbuild.Operation"),
			int64(30),
			mock.MatchedBy(func(simResult entities.RPCSimulateTransactionResult) bool {
				// Verify that TransactionData was successfully parsed and matches our original data
				expectedTxDataBase64, marshalErr := xdr.MarshalBase64(simResult.TransactionData)
				if marshalErr != nil {
					return false
				}
				return expectedTxDataBase64 == validTxDataBase64 &&
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

		// Create a valid operation XDR
		srcAccount := keypair.MustRandom().Address()
		p := txnbuild.Payment{
			Destination:   keypair.MustRandom().Address(),
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: srcAccount,
		}
		op, err := p.BuildXDR()
		require.NoError(t, err)

		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		err = op.EncodeTo(enc)
		require.NoError(t, err)

		opXDR := buf.String()
		operationXDR := base64.StdEncoding.EncodeToString([]byte(opXDR))

		invalidTransactionData := "invalid-transaction-data-xdr"

		input := graphql.BuildTransactionInput{
			Transaction: &graphql.TransactionInput{
				Operations: []string{operationXDR},
				Timeout:    30,
				SimulationResult: &graphql.SimulationResultInput{
					TransactionData: &invalidTransactionData,
				},
			},
		}

		result, err := resolver.BuildTransaction(ctx, input)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorContains(t, err, "Invalid TransactionData")

		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			assert.Equal(t, "INVALID_TRANSACTION_DATA", gqlErr.Extensions["code"])
		}
	})
}
