// Package types defines data structures for the Stellar wallet indexer and GraphQL API.
// This package implements a sophisticated state change architecture that bridges database storage with GraphQL interface requirements.
//
// STATE CHANGE ARCHITECTURE OVERVIEW:
//
// The state change system uses three key components that work together:
//
//  1. DATABASE LAYER: A unified StateChange struct that contains all possible fields for any state change type.
//     This allows efficient storage in a single database table with nullable fields.
//
//  2. GRAPHQL LAYER: A BaseStateChange interface with 8 concrete implementations (defined in
//     internal/serve/graphql/schema/statechange.graphqls). Each type has only the fields relevant to that
//     specific state change category, providing strong typing and clean API contracts.
//
//  3. ADAPTER LAYER: Separate "Model" structs (PaymentStateChangeModel, etc.) that embed the unified
//     StateChange struct. These act as adapters, allowing gqlgen to generate proper resolvers while
//     maintaining the single-table database design.
//
// The conversion between layers happens in internal/serve/graphql/resolvers/utils.go:convertStateChangeTypes(),
// which maps StateChangeCategory values to appropriate concrete GraphQL types.
//
// This design follows the adapter pattern, enabling:
// - Efficient database storage (single table, unified queries)
// - Strong GraphQL typing (interface with specific implementations)
// - Clean separation of concerns (database vs API representations)
// - Type safety in resolver generation (gqlgen requirements satisfied)
//
// See also:
// - internal/serve/graphql/schema/statechange.graphqls (GraphQL interface & types)
// - gqlgen.yml (Go type mappings for GraphQL generation)
// - internal/serve/graphql/resolvers/utils.go (conversion logic)
package types

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/stellar/go/xdr"
)

type Account struct {
	StellarAddress string    `json:"address,omitempty" db:"stellar_address"`
	CreatedAt      time.Time `json:"createdAt,omitempty" db:"created_at"`
}

type AccountWithTxHash struct {
	Account
	TxHash string `json:"txHash,omitempty" db:"tx_hash"`
}

type AccountWithOperationID struct {
	Account
	OperationID int64 `json:"operationId,omitempty" db:"operation_id"`
}

type TrustlineChange struct {
	AccountID    string
	Asset        string
	OperationID  int64
	LedgerNumber uint32
	Operation    TrustlineOpType
}

type TrustlineOpType string

const (
	TrustlineOpAdd    TrustlineOpType = "ADD"
	TrustlineOpRemove TrustlineOpType = "REMOVE"
)

type ContractType string

const (
	ContractTypeNative  ContractType = "NATIVE"
	ContractTypeSAC     ContractType = "SAC"
	ContractTypeSEP41   ContractType = "SEP41"
	ContractTypeUnknown ContractType = "UNKNOWN"
)

type ContractChange struct {
	AccountID    string
	ContractID   string
	OperationID  int64
	LedgerNumber uint32
	ContractType ContractType
}

type Transaction struct {
	Hash            string    `json:"hash,omitempty" db:"hash"`
	ToID            int64     `json:"toId,omitempty" db:"to_id"`
	EnvelopeXDR     string    `json:"envelopeXdr,omitempty" db:"envelope_xdr"`
	ResultXDR       string    `json:"resultXdr,omitempty" db:"result_xdr"`
	MetaXDR         string    `json:"metaXdr,omitempty" db:"meta_xdr"`
	LedgerNumber    uint32    `json:"ledgerNumber,omitempty" db:"ledger_number"`
	LedgerCreatedAt time.Time `json:"ledgerCreatedAt,omitempty" db:"ledger_created_at"`
	IngestedAt      time.Time `json:"ingestedAt,omitempty" db:"ingested_at"`
	// Relationships:
	Operations   []Operation   `json:"operations,omitempty"`
	Accounts     []Account     `json:"accounts,omitempty"`
	StateChanges []StateChange `json:"stateChanges,omitempty"`
}

type TransactionWithCursor struct {
	Transaction
	Cursor int64 `json:"cursor,omitempty" db:"cursor"`
}

type TransactionWithStateChangeID struct {
	Transaction
	StateChangeID string `json:"stateChangeId,omitempty" db:"state_change_id"`
}

type TransactionWithOperationID struct {
	Transaction
	OperationID int64 `json:"operationId,omitempty" db:"operation_id"`
}

// xdrToOperationTypeMap provides 1:1 mapping between XDR OperationType and custom OperationType
var xdrToOperationTypeMap = map[xdr.OperationType]OperationType{
	xdr.OperationTypeCreateAccount:                 OperationTypeCreateAccount,
	xdr.OperationTypePayment:                       OperationTypePayment,
	xdr.OperationTypePathPaymentStrictReceive:      OperationTypePathPaymentStrictReceive,
	xdr.OperationTypeManageSellOffer:               OperationTypeManageSellOffer,
	xdr.OperationTypeCreatePassiveSellOffer:        OperationTypeCreatePassiveSellOffer,
	xdr.OperationTypeSetOptions:                    OperationTypeSetOptions,
	xdr.OperationTypeChangeTrust:                   OperationTypeChangeTrust,
	xdr.OperationTypeAllowTrust:                    OperationTypeAllowTrust,
	xdr.OperationTypeAccountMerge:                  OperationTypeAccountMerge,
	xdr.OperationTypeInflation:                     OperationTypeInflation,
	xdr.OperationTypeManageData:                    OperationTypeManageData,
	xdr.OperationTypeBumpSequence:                  OperationTypeBumpSequence,
	xdr.OperationTypeManageBuyOffer:                OperationTypeManageBuyOffer,
	xdr.OperationTypePathPaymentStrictSend:         OperationTypePathPaymentStrictSend,
	xdr.OperationTypeCreateClaimableBalance:        OperationTypeCreateClaimableBalance,
	xdr.OperationTypeClaimClaimableBalance:         OperationTypeClaimClaimableBalance,
	xdr.OperationTypeBeginSponsoringFutureReserves: OperationTypeBeginSponsoringFutureReserves,
	xdr.OperationTypeEndSponsoringFutureReserves:   OperationTypeEndSponsoringFutureReserves,
	xdr.OperationTypeRevokeSponsorship:             OperationTypeRevokeSponsorship,
	xdr.OperationTypeClawback:                      OperationTypeClawback,
	xdr.OperationTypeClawbackClaimableBalance:      OperationTypeClawbackClaimableBalance,
	xdr.OperationTypeSetTrustLineFlags:             OperationTypeSetTrustLineFlags,
	xdr.OperationTypeLiquidityPoolDeposit:          OperationTypeLiquidityPoolDeposit,
	xdr.OperationTypeLiquidityPoolWithdraw:         OperationTypeLiquidityPoolWithdraw,
	xdr.OperationTypeInvokeHostFunction:            OperationTypeInvokeHostFunction,
	xdr.OperationTypeExtendFootprintTtl:            OperationTypeExtendFootprintTTL,
	xdr.OperationTypeRestoreFootprint:              OperationTypeRestoreFootprint,
}

func OperationTypeFromXDR(xdrOpType xdr.OperationType) OperationType {
	if mappedType, exists := xdrToOperationTypeMap[xdrOpType]; exists {
		return mappedType
	}
	return ""
}

type OperationType string

const (
	OperationTypeCreateAccount                 OperationType = "CREATE_ACCOUNT"
	OperationTypePayment                       OperationType = "PAYMENT"
	OperationTypePathPaymentStrictReceive      OperationType = "PATH_PAYMENT_STRICT_RECEIVE"
	OperationTypeManageSellOffer               OperationType = "MANAGE_SELL_OFFER"
	OperationTypeCreatePassiveSellOffer        OperationType = "CREATE_PASSIVE_SELL_OFFER"
	OperationTypeSetOptions                    OperationType = "SET_OPTIONS"
	OperationTypeChangeTrust                   OperationType = "CHANGE_TRUST"
	OperationTypeAllowTrust                    OperationType = "ALLOW_TRUST"
	OperationTypeAccountMerge                  OperationType = "ACCOUNT_MERGE"
	OperationTypeInflation                     OperationType = "INFLATION"
	OperationTypeManageData                    OperationType = "MANAGE_DATA"
	OperationTypeBumpSequence                  OperationType = "BUMP_SEQUENCE"
	OperationTypeManageBuyOffer                OperationType = "MANAGE_BUY_OFFER"
	OperationTypePathPaymentStrictSend         OperationType = "PATH_PAYMENT_STRICT_SEND"
	OperationTypeCreateClaimableBalance        OperationType = "CREATE_CLAIMABLE_BALANCE"
	OperationTypeClaimClaimableBalance         OperationType = "CLAIM_CLAIMABLE_BALANCE"
	OperationTypeBeginSponsoringFutureReserves OperationType = "BEGIN_SPONSORING_FUTURE_RESERVES"
	OperationTypeEndSponsoringFutureReserves   OperationType = "END_SPONSORING_FUTURE_RESERVES"
	OperationTypeRevokeSponsorship             OperationType = "REVOKE_SPONSORSHIP"
	OperationTypeClawback                      OperationType = "CLAWBACK"
	OperationTypeClawbackClaimableBalance      OperationType = "CLAWBACK_CLAIMABLE_BALANCE"
	OperationTypeSetTrustLineFlags             OperationType = "SET_TRUST_LINE_FLAGS"
	OperationTypeLiquidityPoolDeposit          OperationType = "LIQUIDITY_POOL_DEPOSIT"
	OperationTypeLiquidityPoolWithdraw         OperationType = "LIQUIDITY_POOL_WITHDRAW"
	OperationTypeInvokeHostFunction            OperationType = "INVOKE_HOST_FUNCTION"
	OperationTypeExtendFootprintTTL            OperationType = "EXTEND_FOOTPRINT_TTL"
	OperationTypeRestoreFootprint              OperationType = "RESTORE_FOOTPRINT"
)

type Operation struct {
	ID              int64         `json:"id,omitempty" db:"id"`
	OperationType   OperationType `json:"operationType,omitempty" db:"operation_type"`
	OperationXDR    string        `json:"operationXdr,omitempty" db:"operation_xdr"`
	LedgerNumber    uint32        `json:"ledgerNumber,omitempty" db:"ledger_number"`
	LedgerCreatedAt time.Time     `json:"ledgerCreatedAt,omitempty" db:"ledger_created_at"`
	IngestedAt      time.Time     `json:"ingestedAt,omitempty" db:"ingested_at"`
	// Relationships:
	TxHash       string        `json:"txHash,omitempty" db:"tx_hash"`
	Transaction  *Transaction  `json:"transaction,omitempty"`
	Accounts     []Account     `json:"accounts,omitempty"`
	StateChanges []StateChange `json:"stateChanges,omitempty"`
}

type OperationWithCursor struct {
	Operation
	Cursor int64 `json:"cursor,omitempty" db:"cursor"`
}

type OperationWithStateChangeID struct {
	Operation
	StateChangeID string `db:"state_change_id"`
}

type AccountWithStateChangeID struct {
	Account
	StateChangeID string `db:"state_change_id"`
}

type StateChangeCategory string

const (
	StateChangeCategoryBalance              StateChangeCategory = "BALANCE"
	StateChangeCategoryAccount              StateChangeCategory = "ACCOUNT"
	StateChangeCategorySigner               StateChangeCategory = "SIGNER"
	StateChangeCategorySignatureThreshold   StateChangeCategory = "SIGNATURE_THRESHOLD"
	StateChangeCategoryMetadata             StateChangeCategory = "METADATA"
	StateChangeCategoryFlags                StateChangeCategory = "FLAGS"
	StateChangeCategoryTrustline            StateChangeCategory = "TRUSTLINE"
	StateChangeCategoryReserves             StateChangeCategory = "RESERVES"
	StateChangeCategoryBalanceAuthorization StateChangeCategory = "BALANCE_AUTHORIZATION"
	StateChangeCategoryAuthorization        StateChangeCategory = "AUTHORIZATION"
)

type StateChangeReason string

const (
	StateChangeReasonCreate     StateChangeReason = "CREATE"
	StateChangeReasonMerge      StateChangeReason = "MERGE"
	StateChangeReasonDebit      StateChangeReason = "DEBIT"
	StateChangeReasonCredit     StateChangeReason = "CREDIT"
	StateChangeReasonMint       StateChangeReason = "MINT"
	StateChangeReasonBurn       StateChangeReason = "BURN"
	StateChangeReasonAdd        StateChangeReason = "ADD"
	StateChangeReasonRemove     StateChangeReason = "REMOVE"
	StateChangeReasonUpdate     StateChangeReason = "UPDATE"
	StateChangeReasonLow        StateChangeReason = "LOW"
	StateChangeReasonMedium     StateChangeReason = "MEDIUM"
	StateChangeReasonHigh       StateChangeReason = "HIGH"
	StateChangeReasonHomeDomain StateChangeReason = "HOME_DOMAIN"
	StateChangeReasonSet        StateChangeReason = "SET"
	StateChangeReasonClear      StateChangeReason = "CLEAR"
	StateChangeReasonDataEntry  StateChangeReason = "DATA_ENTRY"
	StateChangeReasonSponsor    StateChangeReason = "SPONSOR"
	StateChangeReasonUnsponsor  StateChangeReason = "UNSPONSOR"
)

// StateChange represents a unified database model for all types of blockchain state changes.
//
// DESIGN RATIONALE:
// This single struct contains all possible fields for any state change type, allowing efficient
// storage in one database table. Most fields are nullable since each state change category only
// uses a subset of the available fields.
//
// FIELD USAGE BY CATEGORY:
// - Payment changes (CREDIT/DEBIT/MINT/BURN): TokenID, Amount, ClaimableBalanceID, LiquidityPoolID
// - Liability changes: TokenID, Amount, OfferID
// - Sponsorship changes: SponsoredAccountID, SponsorAccountID
// - Signer changes: SignerAccountID, SignerWeights
// - Threshold changes: Thresholds
// - Flag changes: Flags
// - Metadata changes: KeyValue
// - Allowance changes: SpenderAccountID
//
// The StateChangeCategory field determines which subset of fields are populated and relevant.
// This approach enables:
// - Single table queries across all state change types
// - Efficient database storage with minimal schema complexity
// - Unified handling in indexer code
// - Easy conversion to strongly-typed GraphQL responses
//
// See convertStateChangeTypes() in internal/serve/graphql/resolvers/utils.go for the mapping
// logic that converts this unified representation to specific GraphQL types.
type StateChange struct {
	ToID                int64               `json:"toId,omitempty" db:"to_id"`
	StateChangeOrder    int64               `json:"stateChangeOrder,omitempty" db:"state_change_order"`
	StateChangeCategory StateChangeCategory `json:"stateChangeCategory,omitempty" db:"state_change_category"`
	StateChangeReason   *StateChangeReason  `json:"stateChangeReason,omitempty" db:"state_change_reason"`
	IngestedAt          time.Time           `json:"ingestedAt,omitempty" db:"ingested_at"`
	LedgerCreatedAt     time.Time           `json:"ledgerCreatedAt,omitempty" db:"ledger_created_at"`
	LedgerNumber        uint32              `json:"ledgerNumber,omitempty" db:"ledger_number"`
	// Nullable fields:
	TokenID            sql.NullString `json:"tokenId,omitempty" db:"token_id"`
	Amount             sql.NullString `json:"amount,omitempty" db:"amount"`
	OfferID            sql.NullString `json:"offerId,omitempty" db:"offer_id"`
	SignerAccountID    sql.NullString `json:"signerAccountId,omitempty" db:"signer_account_id"`
	SpenderAccountID   sql.NullString `json:"spenderAccountId,omitempty" db:"spender_account_id"`
	SponsoredAccountID sql.NullString `json:"sponsoredAccountId,omitempty" db:"sponsored_account_id"`
	SponsorAccountID   sql.NullString `json:"sponsorAccountId,omitempty" db:"sponsor_account_id"`
	DeployerAccountID  sql.NullString `json:"deployerAccountId,omitempty" db:"deployer_account_id"`
	FunderAccountID    sql.NullString `json:"funderAccountId,omitempty" db:"funder_account_id"`
	// Nullable JSONB fields: // TODO: update from `NullableJSONB` to custom objects, except for KeyValue.
	SignerWeights  NullableJSONB `json:"signerWeights,omitempty" db:"signer_weights"`
	Thresholds     NullableJSONB `json:"thresholds,omitempty" db:"thresholds"`
	TrustlineLimit NullableJSONB `json:"trustlineLimit,omitempty" db:"trustline_limit"`
	Flags          NullableJSON  `json:"flags,omitempty" db:"flags"`
	KeyValue       NullableJSONB `json:"keyValue,omitempty" db:"key_value"`
	// Relationships:
	AccountID   string       `json:"accountId,omitempty" db:"account_id"`
	Account     *Account     `json:"account,omitempty"`
	OperationID int64        `json:"operationId,omitempty" db:"operation_id"`
	Operation   *Operation   `json:"operation,omitempty"`
	TxHash      string       `json:"txHash,omitempty" db:"tx_hash"`
	Transaction *Transaction `json:"transaction,omitempty"`
	// Internal IDs used for sorting state changes within an operation.
	SortKey string `json:"-"`
	TxID    int64  `json:"-"`
	// code:issuer formatted asset string
	TrustlineAsset string `json:"-"`
	// Internal only: used for filtering contract changes and identifying token type
	ContractType ContractType `json:"-"`
}

type StateChangeWithCursor struct {
	StateChange
	Cursor StateChangeCursor `db:"cursor"`
}

type StateChangeCursor struct {
	ToID             int64 `db:"cursor_to_id"`
	StateChangeOrder int64 `db:"cursor_state_change_order"`
}

type StateChangeCursorGetter interface {
	GetCursor() StateChangeCursor
}

type NullableJSONB map[string]any

// NullableJSON represents a nullable JSON array of strings
type NullableJSON []string

var _ sql.Scanner = (*NullableJSON)(nil)

func (n *NullableJSON) Scan(value any) error {
	if value == nil {
		*n = nil
		return nil
	}

	switch v := value.(type) {
	case []byte:
		var stringSlice []string
		if err := json.Unmarshal(v, &stringSlice); err != nil {
			return fmt.Errorf("unmarshalling value []byte: %w", err)
		}
		*n = stringSlice
	case string:
		var stringSlice []string
		if err := json.Unmarshal([]byte(v), &stringSlice); err != nil {
			return fmt.Errorf("unmarshalling value string: %w", err)
		}
		*n = stringSlice
	default:
		return fmt.Errorf("unsupported type for JSON array: %T", value)
	}

	return nil
}

var _ driver.Valuer = (*NullableJSON)(nil)

func (n NullableJSON) Value() (driver.Value, error) {
	// Handle nil slice as empty array to avoid null in JSON
	if n == nil {
		return []byte("[]"), nil
	}

	bytes, err := json.Marshal([]string(n))
	if err != nil {
		return nil, fmt.Errorf("marshalling JSON array: %w", err)
	}

	return bytes, nil
}

var _ sql.Scanner = (*NullableJSONB)(nil)

func (n *NullableJSONB) Scan(value any) error {
	if value == nil {
		*n = nil
		return nil
	}

	switch v := value.(type) {
	case []byte:
		if err := json.Unmarshal(v, n); err != nil {
			return fmt.Errorf("unmarshalling value []byte: %w", err)
		}
	case string:
		if err := json.Unmarshal([]byte(v), n); err != nil {
			return fmt.Errorf("unmarshalling value string: %w", err)
		}
	default:
		return fmt.Errorf("unsupported type for JSONB: %T", value)
	}

	return nil
}

var _ driver.Valuer = (*NullableJSONB)(nil)

func (n NullableJSONB) Value() (driver.Value, error) {
	bytes, err := json.Marshal(n)
	if err != nil {
		return nil, fmt.Errorf("marshalling JSONB: %w", err)
	}

	return bytes, nil
}

// BaseStateChange interface implementation methods
//
// These methods implement the GraphQL BaseStateChange interface defined in
// internal/serve/graphql/schema/statechange.graphqls. They provide a common contract
// that all state change types must satisfy, enabling GraphQL interface resolution.
//
// GQLGEN REQUIREMENT:
// The IsBaseStateChange() method is required by gqlgen to identify types that implement
// the BaseStateChange interface. This is used during GraphQL type resolution to determine
// which concrete type to return (PaymentStateChange, LiabilityStateChange, etc.).
//
// INTERFACE METHODS:
// The remaining Get* methods correspond to the shared fields defined in the GraphQL
// BaseStateChange interface. They ensure consistent access patterns across all concrete
// state change implementations.

// IsBaseStateChange marks this type as implementing the GraphQL BaseStateChange interface.
// Required by gqlgen for interface type resolution.
func (sc StateChange) IsBaseStateChange() {}

// GetType returns the category of this state change for GraphQL 'type' field.
// This method satisfies the GraphQL BaseStateChange interface requirement.
func (sc StateChange) GetType() StateChangeCategory {
	return sc.StateChangeCategory
}

// GetReason returns the reason for this state change for GraphQL 'reason' field.
// This method satisfies the GraphQL BaseStateChange interface requirement.
func (sc StateChange) GetReason() StateChangeReason {
	return *sc.StateChangeReason
}

// GetIngestedAt returns when this state change was processed by the indexer.
func (sc StateChange) GetIngestedAt() time.Time {
	return sc.IngestedAt
}

// GetLedgerCreatedAt returns when the ledger containing this state change was created.
func (sc StateChange) GetLedgerCreatedAt() time.Time {
	return sc.LedgerCreatedAt
}

// GetLedgerNumber returns the ledger sequence number where this state change occurred.
func (sc StateChange) GetLedgerNumber() uint32 {
	return sc.LedgerNumber
}

// GetAccount returns the account affected by this state change.
// This relationship is resolved via GraphQL resolver, not direct database join.
func (sc StateChange) GetAccount() *Account {
	return sc.Account
}

// GetOperation returns the operation that caused this state change.
// May be nil for fee-related state changes that don't have associated operations.
func (sc StateChange) GetOperation() *Operation {
	return sc.Operation
}

// GetTransaction returns the transaction containing the operation that caused this state change.
func (sc StateChange) GetTransaction() *Transaction {
	return sc.Transaction
}

// GetCursor returns the cursor for this state change.
func (sc StateChange) GetCursor() StateChangeCursor {
	return StateChangeCursor{
		ToID:             sc.ToID,
		StateChangeOrder: sc.StateChangeOrder,
	}
}

// GraphQL Adapter Model Structs
//
// These structs act as type adapters between the unified database StateChange model
// and the strongly-typed GraphQL interface system. Each represents a specific concrete
// implementation of the BaseStateChange interface.
//
// WHY SEPARATE STRUCTS ARE NEEDED:
//
// 1. GQLGEN REQUIREMENT: gqlgen needs distinct Go types for each GraphQL type to generate
//    proper resolvers. Without these, gqlgen cannot differentiate between PaymentStateChange
//    and LiabilityStateChange in the generated code.
//
// 2. GRAPHQL INTERFACE PATTERN: GraphQL interfaces require concrete implementations with
//    potentially different field sets. While our database uses one unified schema, GraphQL
//    clients expect type-specific fields (e.g., only PaymentStateChange should expose
//    claimableBalanceId, only SponsorshipStateChange should expose sponsorAccountId).
//
// 3. TYPE SAFETY: These distinct types enable compile-time type checking in resolvers
//    and prevent field access errors at runtime.
//
// 4. RESOLVER GENERATION: gqlgen uses these types to generate type-specific resolver methods
//    and marshaling logic for GraphQL responses.
//
// USAGE PATTERN:
// - Database queries return StateChange structs
// - convertStateChangeTypes() in resolvers/utils.go wraps them in appropriate model structs
// - GraphQL resolvers use the model struct's embedded StateChange for field access
// - gqlgen generates type-specific response marshaling based on the wrapper type
//
// This approach maintains the single-table database efficiency while satisfying GraphQL's
// strong typing requirements. See gqlgen.yml lines 170-196 for the GraphQL-to-Go type mappings.

// StandardBalanceStateChangeModel represents payment-related state changes from classic/SAC/SEP41 balances(CREDIT/DEBIT/MINT/BURN).
// Maps to BalanceStateChange in GraphQL schema. Exposes tokenId, amount.
type StandardBalanceStateChangeModel struct {
	StateChange
}

// AccountStateChangeModel represents account state changes.
// Maps to AccountStateChange in GraphQL schema. Exposes tokenId, amount.
type AccountStateChangeModel struct {
	StateChange
}

// SignerStateChangeModel represents account signer changes.
// Maps to SignerStateChange in GraphQL schema. Exposes signerAccountId, signerWeights.
type SignerStateChangeModel struct {
	StateChange
}

// SignerThresholdsStateChangeModel represents signature threshold changes.
// Maps to SignerThresholdsStateChange in GraphQL schema. Exposes thresholds.
type SignerThresholdsStateChangeModel struct {
	StateChange
}

// MetadataStateChangeModel represents account data entry changes.
// Maps to MetadataStateChange in GraphQL schema. Exposes keyValue.
type MetadataStateChangeModel struct {
	StateChange
}

// FlagsStateChangeModel represents account and trustline flag changes.
// Maps to FlagsStateChange in GraphQL schema. Exposes flags array.
type FlagsStateChangeModel struct {
	StateChange
}

// TrustlineStateChangeModel represents trustline changes.
// Maps to TrustlineStateChange in GraphQL schema. Exposes limit.
type TrustlineStateChangeModel struct {
	StateChange
}

// BalanceAuthorizationStateChangeModel represents balance authorization changes.
// Maps to BalanceAuthorizationStateChange in GraphQL schema. Exposes flags array.
type BalanceAuthorizationStateChangeModel struct {
	StateChange
}

// ReservesStateChangeModel represents account reserves sponsorship changes.
// Maps to ReservesStateChange in GraphQL schema. Exposes sponsoredAccountId, sponsorAccountId.
type ReservesStateChangeModel struct {
	StateChange
}
