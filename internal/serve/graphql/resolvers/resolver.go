// GraphQL resolvers package - implements resolver functions for GraphQL schema
// This package contains the business logic for handling GraphQL queries and field resolution
package resolvers

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.
// This is the main resolver struct that gqlgen uses to resolve GraphQL queries.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
	"github.com/stellar/wallet-backend/internal/services"
)

// BalanceReader provides read-only access to account balance data.
// This interface abstracts balance retrieval for trustlines, native XLM, and SAC balances.
type BalanceReader interface {
	GetTrustlineBalances(ctx context.Context, accountAddress string) ([]data.TrustlineBalance, error)
	GetNativeBalance(ctx context.Context, accountAddress string) (*data.NativeBalance, error)
	GetSACBalances(ctx context.Context, accountAddress string) ([]data.SACBalance, error)
}

const (
	MainnetNativeContractAddress = "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
)

// ResolverConfig holds configuration values for the GraphQL resolver.
type ResolverConfig struct {
	MaxAccountsPerBalancesQuery int
	MaxWorkerPoolSize           int
	EnableParticipantFiltering  bool
}

var ErrNotStateChange = errors.New("object is not a StateChange")

// Resolver is the main resolver struct for gqlgen
// It holds dependencies needed by all resolver functions
// gqlgen will embed this struct in generated resolver interfaces
type Resolver struct {
	// models provides access to data layer for database operations
	// This follows dependency injection pattern - resolvers don't create their own DB connections
	models *data.Models
	// accountService provides account management operations
	accountService services.AccountService
	// transactionService provides transaction building and signing operations
	transactionService services.TransactionService
	// feeBumpService provides fee-bump transaction wrapping operations
	feeBumpService             services.FeeBumpService
	rpcService                 services.RPCService
	balanceReader              BalanceReader
	accountContractTokensModel data.AccountContractTokensModelInterface
	contractMetadataService    services.ContractMetadataService
	// metricsService provides metrics collection capabilities
	metricsService metrics.MetricsService
	// pool provides parallel processing capabilities for batch operations
	pool pond.Pool
	// config holds resolver-specific configuration values
	config ResolverConfig
}

// NewResolver creates a new resolver instance with required dependencies
// This constructor is called during server startup to initialize the resolver
// Dependencies are injected here and available to all resolver functions.
func NewResolver(models *data.Models, accountService services.AccountService, transactionService services.TransactionService, feeBumpService services.FeeBumpService, rpcService services.RPCService, balanceReader BalanceReader, accountContractTokensModel data.AccountContractTokensModelInterface, contractMetadataService services.ContractMetadataService, metricsService metrics.MetricsService, config ResolverConfig) *Resolver {
	poolSize := config.MaxWorkerPoolSize
	if poolSize <= 0 {
		poolSize = 100 // default fallback
	}
	return &Resolver{
		models:                     models,
		accountService:             accountService,
		transactionService:         transactionService,
		feeBumpService:             feeBumpService,
		rpcService:                 rpcService,
		balanceReader:              balanceReader,
		accountContractTokensModel: accountContractTokensModel,
		contractMetadataService:    contractMetadataService,
		metricsService:             metricsService,
		pool:                       pond.NewPool(poolSize),
		config:                     config,
	}
}

// Shared field resolver functions
// These functions handle common field resolution patterns to avoid duplication

// resolveNullableString resolves nullable string fields from the database
// Returns pointer to string if valid, nil if null
func (r *Resolver) resolveNullableString(field sql.NullString) *string {
	if field.Valid {
		return &field.String
	}
	return nil
}

// resolveRequiredString resolves required string fields from the database
// Returns empty string if null to satisfy non-nullable GraphQL fields
func (r *Resolver) resolveRequiredString(field sql.NullString) string {
	if field.Valid {
		return field.String
	}
	return ""
}

// resolveRequiredJSONBField resolves JSONB fields that return required strings
// Marshals Go object to JSON string, returns empty string if field is nil
func (r *Resolver) resolveRequiredJSONBField(field interface{}) (string, error) {
	if field == nil {
		return "", nil
	}
	jsonBytes, err := json.Marshal(field)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSONB field: %w", err)
	}
	return string(jsonBytes), nil
}

// Shared resolver functions for BaseStateChange interface
// These functions provide common logic that all state change types can use

// resolveStateChangeAccount resolves the account field for any state change type
// Since state changes have a direct account_id reference, we can fetch the account directly
func (r *Resolver) resolveStateChangeAccount(ctx context.Context, toID int64, stateChangeOrder int64) (*types.Account, error) {
	loaders := ctx.Value(middleware.LoadersKey).(*dataloaders.Dataloaders)
	dbColumns := GetDBColumnsForFields(ctx, types.Account{})

	stateChangeID := fmt.Sprintf("%d-%d", toID, stateChangeOrder)
	loaderKey := dataloaders.AccountColumnsKey{
		StateChangeID: stateChangeID,
		Columns:       strings.Join(dbColumns, ", "),
	}
	account, err := loaders.AccountByStateChangeIDLoader.Load(ctx, loaderKey)
	if err != nil {
		return nil, fmt.Errorf("loading account for state change %s: %w", stateChangeID, err)
	}
	return account, nil
}

// resolveStateChangeOperation resolves the operation field for any state change type
// Reuses the existing logic from the original StateChange resolver
func (r *Resolver) resolveStateChangeOperation(ctx context.Context, toID int64, stateChangeOrder int64) (*types.Operation, error) {
	loaders := ctx.Value(middleware.LoadersKey).(*dataloaders.Dataloaders)
	dbColumns := GetDBColumnsForFields(ctx, types.Operation{})

	stateChangeID := fmt.Sprintf("%d-%d", toID, stateChangeOrder)
	loaderKey := dataloaders.OperationColumnsKey{
		StateChangeID: stateChangeID,
		Columns:       strings.Join(dbColumns, ", "),
	}
	operations, err := loaders.OperationByStateChangeIDLoader.Load(ctx, loaderKey)
	if err != nil {
		return nil, fmt.Errorf("loading operation for state change %s: %w", stateChangeID, err)
	}
	return operations, nil
}

// resolveStateChangeTransaction resolves the transaction field for any state change type
// Reuses the existing logic from the original StateChange resolver
func (r *Resolver) resolveStateChangeTransaction(ctx context.Context, toID int64, stateChangeOrder int64) (*types.Transaction, error) {
	loaders := ctx.Value(middleware.LoadersKey).(*dataloaders.Dataloaders)
	dbColumns := GetDBColumnsForFields(ctx, types.Transaction{})

	stateChangeID := fmt.Sprintf("%d-%d", toID, stateChangeOrder)
	loaderKey := dataloaders.TransactionColumnsKey{
		StateChangeID: stateChangeID,
		Columns:       strings.Join(dbColumns, ", "),
	}
	transaction, err := loaders.TransactionByStateChangeIDLoader.Load(ctx, loaderKey)
	if err != nil {
		return nil, fmt.Errorf("loading transaction for state change %s: %w", stateChangeID, err)
	}
	return transaction, nil
}

// convertSimulationResult converts GraphQL SimulationResultInput to entities.RPCSimulateTransactionResult
func convertSimulationResult(simulationResultInput *graphql1.SimulationResultInput) (entities.RPCSimulateTransactionResult, error) {
	simulationResult := entities.RPCSimulateTransactionResult{
		Events: simulationResultInput.Events,
	}

	if simulationResultInput.MinResourceFee != nil {
		simulationResult.MinResourceFee = *simulationResultInput.MinResourceFee
	}
	if simulationResultInput.Error != nil {
		simulationResult.Error = *simulationResultInput.Error
	}
	if simulationResultInput.LatestLedger != nil {
		simulationResult.LatestLedger = int64(*simulationResultInput.LatestLedger)
	}

	// Handle TransactionData if provided
	if simulationResultInput.TransactionData != nil {
		var txData xdr.SorobanTransactionData
		if txDataErr := xdr.SafeUnmarshalBase64(*simulationResultInput.TransactionData, &txData); txDataErr != nil {
			return entities.RPCSimulateTransactionResult{}, fmt.Errorf("unmarshalling transaction data: %w", txDataErr)
		}
		simulationResult.TransactionData = txData
	}

	return simulationResult, nil
}
