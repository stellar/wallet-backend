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
	"strconv"
	"strings"

	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
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

// convertSimulationResult converts GraphQL SimulationResultInput to entities.RPCSimulateTransactionResult
func convertSimulationResult(simulationResultInput *graphql1.SimulationResultInput) (entities.RPCSimulateTransactionResult, *gqlerror.Error) {
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
			return entities.RPCSimulateTransactionResult{}, &gqlerror.Error{
				Message: fmt.Sprintf("Invalid TransactionData: %s", txDataErr.Error()),
				Extensions: map[string]interface{}{
					"code": "INVALID_TRANSACTION_DATA",
				},
			}
		}
		simulationResult.TransactionData = txData
	}

	return simulationResult, nil
}

// convertMemo converts GraphQL MemoInput to txnbuild.Memo with validation
func convertMemo(memoInput *graphql1.MemoInput) (txnbuild.Memo, *gqlerror.Error) {
	switch memoInput.Type {
	case graphql1.MemoTypeMemoNone:
		return nil, nil
	case graphql1.MemoTypeMemoText:
		if memoInput.Text == nil {
			return nil, &gqlerror.Error{
				Message: "text field is required for MEMO_TEXT",
				Extensions: map[string]interface{}{
					"code": "INVALID_MEMO",
				},
			}
		}
		if len(*memoInput.Text) > 28 {
			return nil, &gqlerror.Error{
				Message: "memo text cannot exceed 28 characters",
				Extensions: map[string]interface{}{
					"code": "INVALID_MEMO",
				},
			}
		}
		return txnbuild.MemoText(*memoInput.Text), nil
	case graphql1.MemoTypeMemoID:
		if memoInput.ID == nil {
			return nil, &gqlerror.Error{
				Message: "id field is required for MEMO_ID",
				Extensions: map[string]interface{}{
					"code": "INVALID_MEMO",
				},
			}
		}
		id, err := strconv.ParseUint(*memoInput.ID, 10, 64)
		if err != nil {
			return nil, &gqlerror.Error{
				Message: fmt.Sprintf("invalid memo id: %s", err.Error()),
				Extensions: map[string]interface{}{
					"code": "INVALID_MEMO",
				},
			}
		}
		return txnbuild.MemoID(id), nil
	case graphql1.MemoTypeMemoHash:
		if memoInput.Hash == nil {
			return nil, &gqlerror.Error{
				Message: "hash field is required for MEMO_HASH",
				Extensions: map[string]interface{}{
					"code": "INVALID_MEMO",
				},
			}
		}
		// Convert hex string to [32]byte
		var hashBytes xdr.Hash
		if err := xdr.SafeUnmarshalBase64(*memoInput.Hash, &hashBytes); err != nil {
			return nil, &gqlerror.Error{
				Message: fmt.Sprintf("invalid memo hash: %s", err.Error()),
				Extensions: map[string]interface{}{
					"code": "INVALID_MEMO",
				},
			}
		}
		return txnbuild.MemoHash(hashBytes), nil
	case graphql1.MemoTypeMemoReturn:
		if memoInput.RetHash == nil {
			return nil, &gqlerror.Error{
				Message: "retHash field is required for MEMO_RETURN",
				Extensions: map[string]interface{}{
					"code": "INVALID_MEMO",
				},
			}
		}
		// Convert hex string to [32]byte
		var retHashBytes xdr.Hash
		if err := xdr.SafeUnmarshalBase64(*memoInput.RetHash, &retHashBytes); err != nil {
			return nil, &gqlerror.Error{
				Message: fmt.Sprintf("invalid memo retHash: %s", err.Error()),
				Extensions: map[string]interface{}{
					"code": "INVALID_MEMO",
				},
			}
		}
		return txnbuild.MemoReturn(retHashBytes), nil
	default:
		return nil, &gqlerror.Error{
			Message: fmt.Sprintf("unsupported memo type: %s", memoInput.Type),
			Extensions: map[string]interface{}{
				"code": "INVALID_MEMO",
			},
		}
	}
}

// convertPreconditions converts GraphQL PreconditionsInput to txnbuild.Preconditions with validation
func convertPreconditions(preconditionsInput *graphql1.PreconditionsInput) (txnbuild.Preconditions, *gqlerror.Error) {
	var preconditions txnbuild.Preconditions

	// Convert TimeBounds
	if preconditionsInput.TimeBounds != nil {
		var minTime, maxTime int64

		if preconditionsInput.TimeBounds.MinTime != nil {
			var err error
			minTime, err = strconv.ParseInt(*preconditionsInput.TimeBounds.MinTime, 10, 64)
			if err != nil {
				return preconditions, &gqlerror.Error{
					Message: fmt.Sprintf("invalid minTime: %s", err.Error()),
					Extensions: map[string]interface{}{
						"code": "INVALID_PRECONDITIONS",
					},
				}
			}
		}

		if preconditionsInput.TimeBounds.MaxTime != nil {
			var err error
			maxTime, err = strconv.ParseInt(*preconditionsInput.TimeBounds.MaxTime, 10, 64)
			if err != nil {
				return preconditions, &gqlerror.Error{
					Message: fmt.Sprintf("invalid maxTime: %s", err.Error()),
					Extensions: map[string]interface{}{
						"code": "INVALID_PRECONDITIONS",
					},
				}
			}
		}

		preconditions.TimeBounds = txnbuild.NewTimebounds(minTime, maxTime)
	}

	// Convert LedgerBounds
	if preconditionsInput.LedgerBounds != nil {
		preconditions.LedgerBounds = &txnbuild.LedgerBounds{
			MinLedger: uint32(*preconditionsInput.LedgerBounds.MinLedger),
			MaxLedger: uint32(*preconditionsInput.LedgerBounds.MaxLedger),
		}
	}

	// Convert MinSequenceNumber
	if preconditionsInput.MinSeqNum != nil {
		minSeqNum, err := strconv.ParseInt(*preconditionsInput.MinSeqNum, 10, 64)
		if err != nil {
			return preconditions, &gqlerror.Error{
				Message: fmt.Sprintf("invalid minSeqNum: %s", err.Error()),
				Extensions: map[string]interface{}{
					"code": "INVALID_PRECONDITIONS",
				},
			}
		}
		preconditions.MinSequenceNumber = &minSeqNum
	}

	// Convert MinSequenceNumberAge
	if preconditionsInput.MinSeqAge != nil {
		minSeqAge, err := strconv.ParseUint(*preconditionsInput.MinSeqAge, 10, 64)
		if err != nil {
			return preconditions, &gqlerror.Error{
				Message: fmt.Sprintf("invalid minSeqAge: %s", err.Error()),
				Extensions: map[string]interface{}{
					"code": "INVALID_PRECONDITIONS",
				},
			}
		}
		preconditions.MinSequenceNumberAge = minSeqAge
	}

	// Convert MinSequenceNumberLedgerGap
	if preconditionsInput.MinSeqLedgerGap != nil {
		preconditions.MinSequenceNumberLedgerGap = uint32(*preconditionsInput.MinSeqLedgerGap)
	}

	// Convert ExtraSigners
	if len(preconditionsInput.ExtraSigners) > 0 {
		for _, signerStr := range preconditionsInput.ExtraSigners {
			// Validate signer address format
			if err := validateStellarSignerKey(signerStr); err != nil {
				return preconditions, &gqlerror.Error{
					Message: fmt.Sprintf("invalid extra signer: %s", err.Error()),
					Extensions: map[string]interface{}{
						"code": "INVALID_PRECONDITIONS",
					},
				}
			}
			preconditions.ExtraSigners = append(preconditions.ExtraSigners, signerStr)
		}
	}

	return preconditions, nil
}

// validateStellarSignerKey validates a stellar signer key format
func validateStellarSignerKey(signerKey string) error {
	if signerKey == "" {
		return fmt.Errorf("signer key is undefined")
	}
	var xdrKey xdr.SignerKey
	if err := xdrKey.SetAddress(signerKey); err != nil {
		return fmt.Errorf("%s is not a valid stellar signer key", signerKey)
	}
	return nil
}
