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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// AddressBytea represents a Stellar address stored as BYTEA in the database.
// Storage format: 33 bytes (1 version byte + 32 raw key bytes)
// Go representation: StrKey string (G.../C...)
type AddressBytea string

// Scan implements sql.Scanner - converts BYTEA (33 bytes) to StrKey string
func (a *AddressBytea) Scan(value any) error {
	if value == nil {
		*a = ""
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", value)
	}
	if len(bytes) != 33 {
		return fmt.Errorf("expected 33 bytes, got %d", len(bytes))
	}
	versionByte := strkey.VersionByte(bytes[0])
	rawKey := bytes[1:33]
	encoded, err := strkey.Encode(versionByte, rawKey)
	if err != nil {
		return fmt.Errorf("encoding stellar address: %w", err)
	}
	*a = AddressBytea(encoded)
	return nil
}

// Value implements driver.Valuer - converts StrKey string to 33-byte []byte
func (a AddressBytea) Value() (driver.Value, error) {
	if a == "" {
		return nil, nil
	}
	versionByte, rawBytes, err := strkey.DecodeAny(string(a))
	if err != nil {
		return nil, fmt.Errorf("decoding stellar address %s: %w", a, err)
	}
	result := make([]byte, 33)
	result[0] = byte(versionByte)
	copy(result[1:], rawBytes)
	return result, nil
}

// String returns the Stellar address as a string.
func (a AddressBytea) String() string {
	return string(a)
}

// NullAddressBytea represents a nullable Stellar address stored as BYTEA in the database.
// Similar to sql.NullString but handles BYTEA encoding/decoding for Stellar addresses.
type NullAddressBytea struct {
	AddressBytea AddressBytea // The Stellar address (G.../C...)
	Valid        bool         // Valid is true if AddressBytea is not NULL
}

// Scan implements sql.Scanner - converts nullable BYTEA (33 bytes) to StrKey string
func (n *NullAddressBytea) Scan(value any) error {
	if value == nil {
		n.AddressBytea, n.Valid = "", false
		return nil
	}
	if err := n.AddressBytea.Scan(value); err != nil {
		return err
	}
	n.Valid = true
	return nil
}

// Value implements driver.Valuer - converts StrKey string to 33-byte []byte or nil
func (n NullAddressBytea) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.AddressBytea.Value()
}

// String returns the Stellar address as a string (convenience accessor).
func (n NullAddressBytea) String() string {
	return string(n.AddressBytea)
}

// HashBytea represents a transaction hash stored as BYTEA in the database.
// Storage format: 32 bytes (raw SHA-256 hash)
// Go representation: hex string (64 characters)
type HashBytea string

// Scan implements sql.Scanner - converts BYTEA (32 bytes) to hex string
func (h *HashBytea) Scan(value any) error {
	if value == nil {
		*h = ""
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", value)
	}
	if len(bytes) != 32 {
		return fmt.Errorf("expected 32 bytes, got %d", len(bytes))
	}
	*h = HashBytea(hex.EncodeToString(bytes))
	return nil
}

// Value implements driver.Valuer - converts hex string to 32-byte []byte
func (h HashBytea) Value() (driver.Value, error) {
	if h == "" {
		return nil, nil
	}
	bytes, err := hex.DecodeString(string(h))
	if err != nil {
		return nil, fmt.Errorf("decoding hex hash %s: %w", h, err)
	}
	if len(bytes) != 32 {
		return nil, fmt.Errorf("invalid hash length: expected 32 bytes, got %d", len(bytes))
	}
	return bytes, nil
}

// String returns the hash as a hex string.
func (h HashBytea) String() string {
	return string(h)
}

type ContractType string

const (
	ContractTypeNative  ContractType = "NATIVE"
	ContractTypeSAC     ContractType = "SAC"
	ContractTypeSEP41   ContractType = "SEP41"
	ContractTypeUnknown ContractType = "UNKNOWN"
)

type TrustlineChange struct {
	AccountID          string
	Asset              string // "CODE:ISSUER" format
	OperationID        int64
	LedgerNumber       uint32
	Operation          TrustlineOpType
	Balance            int64
	Limit              int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	Flags              uint32
}

type TrustlineOpType string

const (
	TrustlineOpAdd    TrustlineOpType = "ADD"
	TrustlineOpRemove TrustlineOpType = "REMOVE"
	TrustlineOpUpdate TrustlineOpType = "UPDATE"
)

type ContractChange struct {
	AccountID    string
	ContractID   string
	OperationID  int64
	LedgerNumber uint32
	ContractType ContractType
}

type AccountChange struct {
	AccountID          string
	OperationID        int64
	LedgerNumber       uint32
	Operation          AccountOpType
	Balance            int64
	MinimumBalance     int64
	BuyingLiabilities  int64
	SellingLiabilities int64
}

type AccountOpType string

const (
	AccountOpCreate AccountOpType = "CREATE"
	AccountOpUpdate AccountOpType = "UPDATE"
	AccountOpRemove AccountOpType = "REMOVE"
)

type SACBalanceChange struct {
	AccountID         string
	ContractID        string
	OperationID       int64
	LedgerNumber      uint32
	Operation         SACBalanceOp
	Balance           string
	IsAuthorized      bool
	IsClawbackEnabled bool
}

type SACBalanceOp string

const (
	SACBalanceOpAdd    SACBalanceOp = "ADD"
	SACBalanceOpUpdate SACBalanceOp = "UPDATE"
	SACBalanceOpRemove SACBalanceOp = "REMOVE"
)

type Account struct {
	StellarAddress AddressBytea `json:"address,omitempty" db:"stellar_address"`
	CreatedAt      time.Time    `json:"createdAt,omitempty" db:"created_at"`
}

type AccountWithToID struct {
	Account
	ToID int64 `json:"toId,omitempty" db:"tx_to_id"`
}

type AccountWithOperationID struct {
	Account
	OperationID int64 `json:"operationId,omitempty" db:"operation_id"`
}

type Transaction struct {
	Hash            HashBytea `json:"hash,omitempty" db:"hash"`
	ToID            int64     `json:"toId,omitempty" db:"to_id"`
	EnvelopeXDR     *string   `json:"envelopeXdr,omitempty" db:"envelope_xdr"`
	FeeCharged      int64     `json:"feeCharged,omitempty" db:"fee_charged"`
	ResultCode      string    `json:"resultCode,omitempty" db:"result_code"`
	MetaXDR         *string   `json:"metaXdr,omitempty" db:"meta_xdr"`
	LedgerNumber    uint32    `json:"ledgerNumber,omitempty" db:"ledger_number"`
	LedgerCreatedAt time.Time `json:"ledgerCreatedAt,omitempty" db:"ledger_created_at"`
	IsFeeBump       bool      `json:"isFeeBump,omitempty" db:"is_fee_bump"`
	IngestedAt      time.Time `json:"ingestedAt,omitempty" db:"ingested_at"`
	// Relationships:
	Operations   []Operation   `json:"operations,omitempty"`
	Accounts     []Account     `json:"accounts,omitempty"`
	StateChanges []StateChange `json:"stateChanges,omitempty"`
	// InnerTransactionHash is the hash of the inner transaction for fee bump transactions,
	// or the transaction hash for regular transactions.
	// This field is transient and not stored in the database.
	InnerTransactionHash string `json:"innerTransactionHash,omitempty" db:"-"`
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
	// ID is the TOID (Total Order ID) per SEP-35, encoding:
	//   - Ledger sequence (bits 63-32)
	//   - Transaction order within ledger (bits 31-12)
	//   - Operation index within transaction (bits 11-0, 1-indexed)
	//
	// The parent transaction's to_id can be derived: ID &^ 0xFFF
	ID              int64         `json:"id,omitempty" db:"id"`
	OperationType   OperationType `json:"operationType,omitempty" db:"operation_type"`
	OperationXDR    string        `json:"operationXdr,omitempty" db:"operation_xdr"`
	ResultCode      string        `json:"resultCode,omitempty" db:"result_code"`
	Successful      bool          `json:"successful,omitempty" db:"successful"`
	LedgerNumber    uint32        `json:"ledgerNumber,omitempty" db:"ledger_number"`
	LedgerCreatedAt time.Time     `json:"ledgerCreatedAt,omitempty" db:"ledger_created_at"`
	IngestedAt      time.Time     `json:"ingestedAt,omitempty" db:"ingested_at"`
	// Relationships:
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

// Flag bitmask constants for encoding/decoding authorization flags.
// These map to the flags SMALLINT column in the state_changes table.
const (
	FlagBitAuthorized                      int16 = 1 << 0 // Bit 0: authorized
	FlagBitAuthRequired                    int16 = 1 << 1 // Bit 1: auth_required
	FlagBitAuthRevocable                   int16 = 1 << 2 // Bit 2: auth_revocable
	FlagBitAuthImmutable                   int16 = 1 << 3 // Bit 3: auth_immutable
	FlagBitAuthClawbackEnabled             int16 = 1 << 4 // Bit 4: auth_clawback_enabled
	FlagBitClawbackEnabled                 int16 = 1 << 5 // Bit 5: clawback_enabled
	FlagBitAuthorizedToMaintainLiabilities int16 = 1 << 6 // Bit 6: authorized_to_maintain_liabilities
)

// flagNameToBit maps flag names to their bitmask values.
var flagNameToBit = map[string]int16{
	"authorized":                         FlagBitAuthorized,
	"auth_required":                      FlagBitAuthRequired,
	"auth_revocable":                     FlagBitAuthRevocable,
	"auth_immutable":                     FlagBitAuthImmutable,
	"auth_clawback_enabled":              FlagBitAuthClawbackEnabled,
	"clawback_enabled":                   FlagBitClawbackEnabled,
	"authorized_to_maintain_liabilities": FlagBitAuthorizedToMaintainLiabilities,
}

// flagBitToName maps bitmask values to flag names (for decoding)
var flagBitToName = map[int16]string{
	FlagBitAuthorized:                      "authorized",
	FlagBitAuthRequired:                    "auth_required",
	FlagBitAuthRevocable:                   "auth_revocable",
	FlagBitAuthImmutable:                   "auth_immutable",
	FlagBitAuthClawbackEnabled:             "auth_clawback_enabled",
	FlagBitClawbackEnabled:                 "clawback_enabled",
	FlagBitAuthorizedToMaintainLiabilities: "authorized_to_maintain_liabilities",
}

// EncodeFlagsToBitmask encodes a slice of flag names to a bitmask value
func EncodeFlagsToBitmask(flags []string) int16 {
	var bitmask int16
	for _, flag := range flags {
		if bit, ok := flagNameToBit[flag]; ok {
			bitmask |= bit
		}
	}
	return bitmask
}

// DecodeBitmaskToFlags decodes a bitmask value to a slice of flag names
func DecodeBitmaskToFlags(bitmask int16) []string {
	var flags []string
	for bit, name := range flagBitToName {
		if bitmask&bit != 0 {
			flags = append(flags, name)
		}
	}
	return flags
}

// StateChange represents a unified database model for all types of blockchain state changes.
//
// DESIGN RATIONALE:
// This single struct contains all possible fields for any state change type, allowing efficient
// storage in one database table. Most fields are nullable since each state change category only
// uses a subset of the available fields.
//
// FIELD USAGE BY CATEGORY:
// - Payment changes (CREDIT/DEBIT/MINT/BURN): TokenID, Amount, ClaimableBalanceID, LiquidityPoolID
// - Sponsorship changes: SponsoredAccountID, SponsorAccountID, ClaimableBalanceID, LiquidityPoolID, SponsoredData
// - Signer changes: SignerAccountID, SignerWeightOld, SignerWeightNew
// - Threshold changes: ThresholdOld, ThresholdNew
// - Flag changes: Flags (bitmask)
// - Metadata changes: KeyValue
// - Allowance changes: SpenderAccountID
// - Trustline changes: TrustlineLimitOld, TrustlineLimitNew, LiquidityPoolID
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

	// Nullable string fields:
	TokenID sql.NullString `json:"tokenId,omitempty" db:"token_id"`
	Amount  sql.NullString `json:"amount,omitempty" db:"amount"`

	// Nullable address fields (stored as BYTEA in database):
	SignerAccountID    NullAddressBytea `json:"signerAccountId,omitempty" db:"signer_account_id"`
	SpenderAccountID   NullAddressBytea `json:"spenderAccountId,omitempty" db:"spender_account_id"`
	SponsoredAccountID NullAddressBytea `json:"sponsoredAccountId,omitempty" db:"sponsored_account_id"`
	SponsorAccountID   NullAddressBytea `json:"sponsorAccountId,omitempty" db:"sponsor_account_id"`
	DeployerAccountID  NullAddressBytea `json:"deployerAccountId,omitempty" db:"deployer_account_id"`
	FunderAccountID    NullAddressBytea `json:"funderAccountId,omitempty" db:"funder_account_id"`

	// Entity identifiers (moved from key_value JSONB):
	ClaimableBalanceID sql.NullString `json:"claimableBalanceId,omitempty" db:"claimable_balance_id"`
	LiquidityPoolID    sql.NullString `json:"liquidityPoolId,omitempty" db:"liquidity_pool_id"`
	SponsoredData      sql.NullString `json:"sponsoredData,omitempty" db:"sponsored_data"`

	// Flattened signer weights (range 0-255, was JSONB {"old": int, "new": int}):
	SignerWeightOld sql.NullInt16 `json:"signerWeightOld,omitempty" db:"signer_weight_old"`
	SignerWeightNew sql.NullInt16 `json:"signerWeightNew,omitempty" db:"signer_weight_new"`

	// Flattened thresholds (range 0-255, was JSONB {"old": "val", "new": "val"}):
	ThresholdOld sql.NullInt16 `json:"thresholdOld,omitempty" db:"threshold_old"`
	ThresholdNew sql.NullInt16 `json:"thresholdNew,omitempty" db:"threshold_new"`

	// Flattened trustline limit (was JSONB {"limit": {"old": "...", "new": "..."}}):
	TrustlineLimitOld sql.NullString `json:"trustlineLimitOld,omitempty" db:"trustline_limit_old"`
	TrustlineLimitNew sql.NullString `json:"trustlineLimitNew,omitempty" db:"trustline_limit_new"`

	// Flags as bitmask instead of JSON array (see FlagBit* constants):
	Flags sql.NullInt16 `json:"flags,omitempty" db:"flags"`

	// ONLY truly variable data remains as JSONB (data entries, home domain):
	KeyValue NullableJSONB `json:"keyValue,omitempty" db:"key_value"`

	// Relationships:
	AccountID   AddressBytea `json:"accountId,omitempty" db:"account_id"`
	Account     *Account     `json:"account,omitempty"`
	OperationID int64        `json:"operationId,omitempty" db:"operation_id"`
	Operation   *Operation   `json:"operation,omitempty"`
	Transaction *Transaction `json:"transaction,omitempty"`

	// Internal IDs used for sorting state changes within an operation.
	SortKey string `json:"-"`
	// Internal only: used for filtering contract changes and identifying token type
	ContractType ContractType `json:"-"`
}

type StateChangeWithCursor struct {
	StateChange
	Cursor StateChangeCursor `db:"cursor"`
}

type StateChangeCursor struct {
	ToID             int64 `db:"cursor_to_id"`
	OperationID      int64 `db:"cursor_operation_id"`
	StateChangeOrder int64 `db:"cursor_state_change_order"`
}

type StateChangeCursorGetter interface {
	GetCursor() StateChangeCursor
}

type NullableJSONB map[string]any

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
		OperationID:      sc.OperationID,
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
