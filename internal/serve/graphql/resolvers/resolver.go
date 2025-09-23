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

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
	"github.com/stellar/wallet-backend/internal/services"

	// TODO: Move TransactionService under /services
	txservices "github.com/stellar/wallet-backend/internal/transactions/services"
)

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
	transactionService txservices.TransactionService
	// feeBumpService provides fee-bump transaction wrapping operations
	feeBumpService services.FeeBumpService
}

// NewResolver creates a new resolver instance with required dependencies
// This constructor is called during server startup to initialize the resolver
// Dependencies are injected here and available to all resolver functions.
func NewResolver(models *data.Models, accountService services.AccountService, transactionService txservices.TransactionService, feeBumpService services.FeeBumpService) *Resolver {
	return &Resolver{
		models:             models,
		accountService:     accountService,
		transactionService: transactionService,
		feeBumpService:     feeBumpService,
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

// resolveJSONBField resolves JSONB fields that return nullable strings
// Marshals Go object to JSON string, returns nil if field is nil or empty map
func (r *Resolver) resolveJSONBField(field interface{}) (*string, error) {
	if field == nil {
		return nil, nil
	}

	// Handle NullableJSONB specifically - if it's an empty map, treat as nil
	if jsonbField, ok := field.(types.NullableJSONB); ok {
		if len(jsonbField) == 0 {
			return nil, nil
		}
	}

	jsonBytes, err := json.Marshal(field)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSONB field: %w", err)
	}
	jsonString := string(jsonBytes)
	return &jsonString, nil
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
