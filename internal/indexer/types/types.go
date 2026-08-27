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
//  2. GRAPHQL LAYER: A BaseStateChange interface with 19 concrete implementations (defined in
//     internal/serve/graphql/schema/statechange.graphqls). Each type has only the fields relevant to that
//     specific state change, providing strong typing and clean API contracts.
//
//  3. ADAPTER LAYER: Separate "Model" structs (BalanceChangeModel, etc.) that embed the unified
//     StateChange struct. These act as adapters, allowing gqlgen to generate proper resolvers while
//     maintaining the single-table database design.
//
// The conversion between layers happens in internal/serve/graphql/resolvers/utils.go:convertStateChangeTypes(),
// which maps each StateChange to a concrete GraphQL type by its (category, reason) pair.
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
	"encoding/base64"
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

// Value implements driver.Valuer - converts StrKey string to 33-byte []byte.
//
// Muxed addresses (M...) carry a 40-byte payload — a 32-byte ed25519 key followed by an
// 8-byte multiplexing id (SEP-23). They are reduced to their base account here: the id is
// off-chain routing metadata, not part of the stored key. This is a storage-layer backstop;
// ingestion is expected to normalize muxed addresses to their base account before this point
// (see the SEP-41 processor and the classic path's MuxedAccount.ToAccountId()). Any other
// payload length is rejected rather than silently truncated, so a malformed address surfaces
// as an error instead of corrupt data.
func (a AddressBytea) Value() (driver.Value, error) {
	if a == "" {
		return nil, nil
	}
	versionByte, rawBytes, err := strkey.DecodeAny(string(a))
	if err != nil {
		return nil, fmt.Errorf("decoding stellar address %s: %w", a, err)
	}
	if versionByte == strkey.VersionByteMuxedAccount {
		// Guard the payload length before slicing, since the decoder may not enforce
		// it: a muxed payload must be exactly 40 bytes (32-byte ed25519 key + 8-byte id).
		if len(rawBytes) != 40 {
			return nil, fmt.Errorf("stellar muxed address %s has a %d-byte payload; expected 40 bytes", a, len(rawBytes))
		}
		versionByte = strkey.VersionByteAccountID
		rawBytes = rawBytes[:32] // keep the ed25519 key; drop the trailing 8-byte id
	}
	if len(rawBytes) != 32 {
		return nil, fmt.Errorf("stellar address %s has a %d-byte key payload; the account_id column stores only 32-byte account/contract keys", a, len(rawBytes))
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

// XDRBytea represents XDR data stored as BYTEA in the database.
// Storage format: raw XDR bytes (variable length)
// Go representation: raw bytes internally, base64 string via String()
type XDRBytea []byte

// Scan implements sql.Scanner - reads raw bytes from BYTEA column
func (x *XDRBytea) Scan(value any) error {
	if value == nil {
		*x = nil
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", value)
	}
	*x = make([]byte, len(bytes))
	copy(*x, bytes)
	return nil
}

// Value implements driver.Valuer - returns raw bytes for BYTEA storage
func (x XDRBytea) Value() (driver.Value, error) {
	if len(x) == 0 {
		return nil, nil
	}
	buf := make([]byte, len(x))
	copy(buf, x)
	return buf, nil
}

// String returns the XDR as a base64 string.
func (x XDRBytea) String() string {
	return base64.StdEncoding.EncodeToString(x)
}

type ContractType string

const (
	ContractTypeNative  ContractType = "NATIVE"
	ContractTypeSAC     ContractType = "SAC"
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
	AccountID string
	// SortKey is a within-ledger dedup rank (phase|tx|op via accountSortKey), not a TOID.
	// The buffer keeps the highest per account to pick the chronologically-last balance;
	// it is never persisted.
	SortKey      int64
	LedgerNumber uint32
	Operation    AccountOpType
	Balance      int64
	// MinimumBalance is the base reserve requirement in stroops (excludes liabilities):
	// (2 + NumSubEntries + numSponsoring - numSponsored) * baseReserve; matches stellar-core getMinBalance.
	MinimumBalance     int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	NumSubEntries      uint32
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

// LiquidityPoolShareChange is an account's pool-share balance change extracted from a
// pool_share trustline. PoolID is the hex-encoded pool id; Shares is the trustline balance.
type LiquidityPoolShareChange struct {
	AccountID    string
	PoolID       string
	OperationID  int64
	LedgerNumber uint32
	Operation    LiquidityPoolShareOp
	Shares       int64
}

type LiquidityPoolShareOp string

const (
	LiquidityPoolShareOpAdd    LiquidityPoolShareOp = "ADD"
	LiquidityPoolShareOpUpdate LiquidityPoolShareOp = "UPDATE"
	LiquidityPoolShareOpRemove LiquidityPoolShareOp = "REMOVE"
)

// LiquidityPoolChange is a constant-product pool's reserve change extracted from a
// LiquidityPoolEntry ledger entry. PoolID is the hex-encoded pool id; AssetA/AssetB are
// canonical asset strings ("native" or "CODE:ISSUER").
type LiquidityPoolChange struct {
	PoolID       string
	OperationID  int64
	LedgerNumber uint32
	Operation    LiquidityPoolOp
	AssetA       string
	ReserveA     int64
	AssetB       string
	ReserveB     int64
}

type LiquidityPoolOp string

const (
	LiquidityPoolOpAdd    LiquidityPoolOp = "ADD"
	LiquidityPoolOpUpdate LiquidityPoolOp = "UPDATE"
	LiquidityPoolOpRemove LiquidityPoolOp = "REMOVE"
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
	FeeCharged      int64     `json:"feeCharged,omitempty" db:"fee_charged"`
	ResultCode      string    `json:"resultCode,omitempty" db:"result_code"`
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

// CompositeCursor encodes both ledger_created_at and an entity ID for TimescaleDB-friendly
// cursor-based pagination. Using ledger_created_at as the leading sort column allows
// TimescaleDB to use ChunkAppend optimization on hypertables.
type CompositeCursor struct {
	LedgerCreatedAt time.Time `db:"cursor_ledger_created_at"`
	ID              int64     `db:"cursor_id"`
}

type TransactionWithCursor struct {
	Transaction
	CompositeCursor
}

// AccountTransactionEdge backs the GraphQL AccountTransactionEdge. AccountAddress is not part of
// the schema; it scopes the edge's operations/stateChanges resolvers to the account whose
// transactions were queried (the resolver stamps it from the parent Account).
type AccountTransactionEdge struct {
	Node           *Transaction
	Cursor         string
	AccountAddress AddressBytea
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
	OperationXDR    XDRBytea      `json:"operationXdr,omitempty" db:"operation_xdr"`
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
	CompositeCursor
}

type OperationWithStateChangeID struct {
	Operation
	StateChangeID string `db:"state_change_id"`
}

type StateChangeCategory string

const (
	StateChangeCategoryBalance              StateChangeCategory = "BALANCE"
	StateChangeCategoryAccount              StateChangeCategory = "ACCOUNT"
	StateChangeCategorySigner               StateChangeCategory = "SIGNER"
	StateChangeCategorySignatureThreshold   StateChangeCategory = "SIGNATURE_THRESHOLD"
	StateChangeCategoryDataEntry            StateChangeCategory = "DATA_ENTRY"
	StateChangeCategoryHomeDomain           StateChangeCategory = "HOME_DOMAIN"
	StateChangeCategoryAllowance            StateChangeCategory = "ALLOWANCE"
	StateChangeCategoryFlags                StateChangeCategory = "FLAGS"
	StateChangeCategoryTrustline            StateChangeCategory = "TRUSTLINE"
	StateChangeCategoryBalanceAuthorization StateChangeCategory = "BALANCE_AUTHORIZATION"
)

type StateChangeReason string

const (
	StateChangeReasonCreate StateChangeReason = "CREATE"
	StateChangeReasonMerge  StateChangeReason = "MERGE"
	StateChangeReasonDebit  StateChangeReason = "DEBIT"
	StateChangeReasonCredit StateChangeReason = "CREDIT"
	StateChangeReasonMint   StateChangeReason = "MINT"
	StateChangeReasonBurn   StateChangeReason = "BURN"
	StateChangeReasonAdd    StateChangeReason = "ADD"
	StateChangeReasonRemove StateChangeReason = "REMOVE"
	StateChangeReasonUpdate StateChangeReason = "UPDATE"
	StateChangeReasonSet    StateChangeReason = "SET"
	StateChangeReasonClear  StateChangeReason = "CLEAR"
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

// AccountFlag is a Stellar account authorization flag as exposed by the GraphQL AccountFlag enum.
type AccountFlag string

const (
	AccountFlagAuthRequired        AccountFlag = "AUTH_REQUIRED"
	AccountFlagAuthRevocable       AccountFlag = "AUTH_REVOCABLE"
	AccountFlagAuthImmutable       AccountFlag = "AUTH_IMMUTABLE"
	AccountFlagAuthClawbackEnabled AccountFlag = "AUTH_CLAWBACK_ENABLED"
)

// ThresholdLevel identifies which of an account's three signature thresholds a
// ThresholdChange refers to, as exposed by the GraphQL ThresholdLevel enum.
type ThresholdLevel string

const (
	ThresholdLevelLow    ThresholdLevel = "LOW"
	ThresholdLevelMedium ThresholdLevel = "MEDIUM"
	ThresholdLevelHigh   ThresholdLevel = "HIGH"
)

// TrustlineFlag is a Stellar trustline authorization flag as exposed by the GraphQL TrustlineFlag enum.
type TrustlineFlag string

const (
	TrustlineFlagAuthorized                      TrustlineFlag = "AUTHORIZED"
	TrustlineFlagAuthorizedToMaintainLiabilities TrustlineFlag = "AUTHORIZED_TO_MAINTAIN_LIABILITIES"
	TrustlineFlagClawbackEnabled                 TrustlineFlag = "CLAWBACK_ENABLED"
)

// accountFlagBits lists the account authorization bits in fixed decode order, so
// DecodeAccountFlags returns a deterministic slice.
var accountFlagBits = []struct {
	bit  int16
	flag AccountFlag
}{
	{FlagBitAuthRequired, AccountFlagAuthRequired},
	{FlagBitAuthRevocable, AccountFlagAuthRevocable},
	{FlagBitAuthImmutable, AccountFlagAuthImmutable},
	{FlagBitAuthClawbackEnabled, AccountFlagAuthClawbackEnabled},
}

// trustlineFlagBits lists the trustline authorization bits in fixed decode order, so
// DecodeTrustlineFlags returns a deterministic slice.
var trustlineFlagBits = []struct {
	bit  int16
	flag TrustlineFlag
}{
	{FlagBitAuthorized, TrustlineFlagAuthorized},
	{FlagBitAuthorizedToMaintainLiabilities, TrustlineFlagAuthorizedToMaintainLiabilities},
	{FlagBitClawbackEnabled, TrustlineFlagClawbackEnabled},
}

// DecodeAccountFlags decodes only the account authorization bits of bitmask into
// AccountFlag values, in fixed order. Returns nil when no account bits are set.
func DecodeAccountFlags(bitmask int16) []AccountFlag {
	var flags []AccountFlag
	for _, fb := range accountFlagBits {
		if bitmask&fb.bit != 0 {
			flags = append(flags, fb.flag)
		}
	}
	return flags
}

// DecodeTrustlineFlags decodes only the trustline authorization bits of bitmask into
// TrustlineFlag values, in fixed order. Returns nil when no trustline bits are set.
func DecodeTrustlineFlags(bitmask int16) []TrustlineFlag {
	var flags []TrustlineFlag
	for _, fb := range trustlineFlagBits {
		if bitmask&fb.bit != 0 {
			flags = append(flags, fb.flag)
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
// - BALANCE: TokenID, Amount, ToMuxedID (transaction-fee rows have OperationID=0)
// - ACCOUNT: CreatorAccountID (CREATE), DestinationAccountID (MERGE)
// - SIGNER: SignerAccountID, SignerWeightOld, SignerWeightNew
// - SIGNATURE_THRESHOLD: Threshold (which one), ThresholdOld, ThresholdNew
// - DATA_ENTRY: DataEntryName, KeyValue ({"old", "new"})
// - HOME_DOMAIN: KeyValue ({"old"} cleared, {"new"} set, both on update)
// - ALLOWANCE: TokenID, Amount, SpenderAccountID, KeyValue (live_until_ledger)
// - FLAGS: Flags (bitmask)
// - TRUSTLINE: TokenID xor LiquidityPoolID, TrustlineLimitOld, TrustlineLimitNew
// - BALANCE_AUTHORIZATION: TokenID xor LiquidityPoolID, Flags
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
	StateChangeID       int64               `json:"stateChangeId,omitempty" db:"state_change_id"`
	StateChangeCategory StateChangeCategory `json:"stateChangeCategory,omitempty" db:"state_change_category"`
	StateChangeReason   StateChangeReason   `json:"stateChangeReason,omitempty" db:"state_change_reason"`
	IngestedAt          time.Time           `json:"ingestedAt,omitempty" db:"ingested_at"`
	LedgerCreatedAt     time.Time           `json:"ledgerCreatedAt,omitempty" db:"ledger_created_at"`
	LedgerNumber        uint32              `json:"ledgerNumber,omitempty" db:"ledger_number"`

	// Nullable address fields (stored as BYTEA in database):
	TokenID NullAddressBytea `json:"tokenId,omitempty" db:"token_id"`
	Amount  sql.NullString   `json:"amount,omitempty" db:"amount"`

	// CAP-67 destination memo (u64) carried by SEP-41 transfer/mint events; stored as TEXT
	// because u64 values above 2^63-1 would overflow BIGINT.
	ToMuxedID sql.NullString `json:"toMuxedId,omitempty" db:"to_muxed_id"`

	// Nullable address fields (stored as BYTEA in database):
	SignerAccountID      NullAddressBytea `json:"signerAccountId,omitempty" db:"signer_account_id"`
	SpenderAccountID     NullAddressBytea `json:"spenderAccountId,omitempty" db:"spender_account_id"`
	CreatorAccountID     NullAddressBytea `json:"creatorAccountId,omitempty" db:"creator_account_id"`
	DestinationAccountID NullAddressBytea `json:"destinationAccountId,omitempty" db:"destination_account_id"`

	// Entity identifiers (moved from key_value JSONB):
	LiquidityPoolID sql.NullString `json:"liquidityPoolId,omitempty" db:"liquidity_pool_id"`

	// Flattened signer weights (range 0-255, was JSONB {"old": int, "new": int}):
	SignerWeightOld sql.NullInt16 `json:"signerWeightOld,omitempty" db:"signer_weight_old"`
	SignerWeightNew sql.NullInt16 `json:"signerWeightNew,omitempty" db:"signer_weight_new"`

	// Signature thresholds: which threshold changed (ThresholdLevel value) plus
	// its old/new values (range 0-255). One row per changed threshold.
	Threshold    sql.NullString `json:"threshold,omitempty" db:"threshold"`
	ThresholdOld sql.NullInt16  `json:"thresholdOld,omitempty" db:"threshold_old"`
	ThresholdNew sql.NullInt16  `json:"thresholdNew,omitempty" db:"threshold_new"`

	// Flattened trustline limit (was JSONB {"limit": {"old": "...", "new": "..."}}):
	TrustlineLimitOld sql.NullString `json:"trustlineLimitOld,omitempty" db:"trustline_limit_old"`
	TrustlineLimitNew sql.NullString `json:"trustlineLimitNew,omitempty" db:"trustline_limit_new"`

	// Flags as bitmask instead of JSON array (see FlagBit* constants):
	Flags sql.NullInt16 `json:"flags,omitempty" db:"flags"`

	// Name of the changed data entry (DATA_ENTRY rows only):
	DataEntryName sql.NullString `json:"dataEntryName,omitempty" db:"data_entry_name"`

	// ONLY truly variable data remains as JSONB, as a flat {"old", "new"} payload
	// (data-entry values, home domain) plus the allowance live_until_ledger:
	KeyValue NullableJSONB `json:"keyValue,omitempty" db:"key_value"`

	// Relationships:
	AccountID   AddressBytea `json:"accountId,omitempty" db:"account_id"`
	Account     *Account     `json:"account,omitempty"`
	OperationID int64        `json:"operationId,omitempty" db:"operation_id"`
	Operation   *Operation   `json:"operation,omitempty"`
	Transaction *Transaction `json:"transaction,omitempty"`
}

type StateChangeWithCursor struct {
	StateChange
	StateChangeCursor
}

type StateChangeCursor struct {
	LedgerCreatedAt time.Time `db:"cursor_ledger_created_at"`
	ToID            int64     `db:"cursor_to_id"`
	OperationID     int64     `db:"cursor_operation_id"`
	StateChangeID   int64     `db:"cursor_state_change_id"`
}

type StateChangeCursorGetter interface {
	GetCursor() StateChangeCursor
}

// State change ID namespace registry.
//
// state_changes rows are written by more than one independent emitter: the
// main indexer (effects, token transfers, etc.) and protocol processors
// (SEP-41, Blend) that persist their own history inside a separately
// CAS-guarded transaction. Two emitters can legitimately target the same
// (to_id, operation_id) — e.g. a SEP-41 or Blend contract invocation is one
// operation the main indexer also sees. Each emitter numbers its own state
// changes 1..N in deterministic emission order within an (to_id, operation_id)
// group (see AssignStateChangeOrdinals) and adds its base below, so the
// resulting state_change_id values can never collide across emitters even
// when they share an operation. Bases are consecutive multiples of
// StateChangeOrdinalNamespaceWidth and must never change once rows exist with
// them.
//
// Why the split at bit 40: an int64 has 63 usable positive bits. Reserving the
// low 40 bits per namespace carves the space into 2^23 (~8.4M) emitter
// namespaces, each holding 2^40 (~1.1e12) ordinals per (to_id, operation_id)
// group. Transaction meta size limits cap a single operation's emissions at a
// few thousand, and new emitters are added only at code-review speed, so both
// sides of the split sit orders of magnitude beyond their physical bounds —
// neither dimension can become the binding constraint, and frozen bases never
// face renumbering pressure.
//
// The indexer subdivides its namespace further, one sub-namespace per
// emitting processor — see the sub-base registry below.
const (
	// StateChangeOrdinalNamespaceWidth is the span of state_change_id values
	// reserved for each emitter namespace; the bases below are consecutive
	// multiples of it. Changing it renumbers every base and is forbidden once
	// rows exist.
	StateChangeOrdinalNamespaceWidth int64 = 1 << 40

	StateChangeOrdinalBaseIndexer int64 = 0
	StateChangeOrdinalBaseSEP41   int64 = 1 * StateChangeOrdinalNamespaceWidth
	StateChangeOrdinalBaseBlend   int64 = 2 * StateChangeOrdinalNamespaceWidth
)

// Sub-namespace registry within the indexer's emitter namespace.
//
// The indexer is itself multi-stream: several processors (token transfers,
// effects, contract deploys, SAC events) can emit state changes for the same
// operation. Each processor numbers its own emissions independently (see
// Indexer.getTransactionStateChanges) and adds StateChangeOrdinalBaseIndexer
// plus its sub-base below, so IDs never depend on the order processors are
// registered or invoked in — adding or reordering processors cannot shift
// another processor's IDs. Within one processor, emission order derives from
// the transaction meta (canonical on-chain data), which is what makes the
// scheme reproducible across runs.
//
// Sub-bases split the indexer's 1<<40-wide namespace at bit 28: 2^12 = 4096
// sub-streams fit inside it, each with 2^28 (~268M) ordinals per
// (to_id, operation_id) group. As with the bit-40 emitter split, both sides
// sit orders of magnitude beyond their physical bounds — transaction meta size
// limits cap per-operation emissions at a few thousand, and new sub-streams
// are added only at code-review speed — so neither dimension can become the
// binding constraint. Protocol emitters (SEP-41, Blend) are single-stream and use their
// base directly; a future multi-stream emitter subdivides its own namespace
// the same way. Like the emitter bases, sub-bases must never change once
// rows exist with them; new processors take the next unused slot.
const (
	// StateChangeSubNamespaceWidth is the span of state_change_id values
	// reserved for each indexer sub-stream; the sub-bases below are consecutive
	// multiples of it. Changing it renumbers every sub-base and is forbidden
	// once rows exist.
	StateChangeSubNamespaceWidth int64 = 1 << 28

	StateChangeSubBaseTokenTransfer  int64 = 0 * StateChangeSubNamespaceWidth
	StateChangeSubBaseEffects        int64 = 1 * StateChangeSubNamespaceWidth
	StateChangeSubBaseContractDeploy int64 = 2 * StateChangeSubNamespaceWidth
	StateChangeSubBaseSACEvents      int64 = 3 * StateChangeSubNamespaceWidth
)

// AssignStateChangeOrdinals assigns each state change in changes a
// deterministic state_change_id: an ordinal numbered 1..N in slice order
// within each distinct (to_id, operation_id) group, plus base. base is the calling
// emitter's namespace base — plus its processor's sub-base for multi-stream
// emitters like the indexer. Processing the same input twice (e.g. a
// re-ingested ledger) yields byte-identical IDs, so a duplicate BatchCopy
// fails loudly on the state_changes primary key instead of inserting
// duplicate rows.
//
// Callers must pass one emission stream at a time — the final, filtered slice
// actually handed to BatchCopy — so ordinals come out contiguous (1..N, no
// gaps) per group within the stream's namespace.
func AssignStateChangeOrdinals(changes []StateChange, base int64) {
	type ordinalKey struct{ toID, opID int64 }
	next := make(map[ordinalKey]int64, len(changes))
	for i := range changes {
		k := ordinalKey{changes[i].ToID, changes[i].OperationID}
		next[k]++
		changes[i].StateChangeID = base + next[k]
	}
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
// which concrete type to return (BalanceChange, SignerAddedChange, etc.).
//
// INTERFACE METHODS:
// The remaining Get* methods correspond to the shared fields defined in the GraphQL
// BaseStateChange interface. They ensure consistent access patterns across all concrete
// state change implementations.

// IsBaseStateChange marks this type as implementing the GraphQL BaseStateChange interface.
// Required by gqlgen for interface type resolution.
func (sc StateChange) IsBaseStateChange() {}

// GetCategory returns the category of this state change for the GraphQL 'category' field.
// This method satisfies the GraphQL BaseStateChange interface requirement.
func (sc StateChange) GetCategory() StateChangeCategory {
	return sc.StateChangeCategory
}

// GetReason returns the reason for this state change for GraphQL 'reason' field.
// This method satisfies the GraphQL BaseStateChange interface requirement.
func (sc StateChange) GetReason() StateChangeReason {
	return sc.StateChangeReason
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
// Nil for transaction-fee rows (no operation).
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
		LedgerCreatedAt: sc.LedgerCreatedAt,
		ToID:            sc.ToID,
		OperationID:     sc.OperationID,
		StateChangeID:   sc.StateChangeID,
	}
}

// GraphQL Adapter Model Structs
//
// These structs act as type adapters between the unified database StateChange model
// and the strongly-typed GraphQL interface system. Each represents a specific concrete
// implementation of the BaseStateChange interface, selected by (category, reason) and,
// where those alone are ambiguous, additional discriminator fields.
//
// WHY SEPARATE STRUCTS ARE NEEDED:
//
// 1. GQLGEN REQUIREMENT: gqlgen needs distinct Go types for each GraphQL type to generate
//    proper resolvers. Without these, gqlgen cannot differentiate between BalanceChange
//    and AllowanceChange in the generated code.
//
// 2. GRAPHQL INTERFACE PATTERN: GraphQL interfaces require concrete implementations with
//    potentially different field sets. While our database uses one unified schema, GraphQL
//    clients expect type-specific fields (e.g., only BalanceChange should expose
//    toMuxedId, only AccountMergedChange should expose destinationAddress).
//
// 3. TYPE SAFETY: These distinct types enable compile-time type checking in resolvers
//    and prevent field access errors at runtime.
//
// 4. RESOLVER GENERATION: gqlgen uses these types to generate type-specific resolver methods
//    and marshaling logic for GraphQL responses.
//
// USAGE PATTERN:
// - Database queries return StateChange structs
// - convertStateChangeTypes() in internal/serve/graphql/resolvers/utils.go wraps them in the
//   appropriate model struct based on (category, reason)
// - GraphQL resolvers use the model struct's embedded StateChange for field access
// - gqlgen generates type-specific response marshaling based on the wrapper type
//
// This approach maintains the single-table database efficiency while satisfying GraphQL's
// strong typing requirements. See the models section of gqlgen.yml for the GraphQL-to-Go
// type mappings.

// BalanceChangeModel maps to GraphQL BalanceChange: BALANCE × DEBIT/CREDIT/MINT/BURN.
// Covers operation-sourced movements and transaction-fee rows (OperationID=0).
type BalanceChangeModel struct {
	StateChange
}

// AccountCreatedChangeModel maps to GraphQL AccountCreatedChange: ACCOUNT × CREATE.
// Covers both classic account creation (Account holds the created G-address) and
// contract deployment (Account holds the deployed C-address); CreatorAccountID is the
// funding account or the deploying address respectively.
type AccountCreatedChangeModel struct {
	StateChange
}

// AccountMergedChangeModel maps to GraphQL AccountMergedChange: ACCOUNT × MERGE.
type AccountMergedChangeModel struct {
	StateChange
}

// SignerAddedChangeModel maps to GraphQL SignerAddedChange: SIGNER × ADD.
type SignerAddedChangeModel struct {
	StateChange
}

// SignerUpdatedChangeModel maps to GraphQL SignerUpdatedChange: SIGNER × UPDATE.
type SignerUpdatedChangeModel struct {
	StateChange
}

// SignerRemovedChangeModel maps to GraphQL SignerRemovedChange: SIGNER × REMOVE.
type SignerRemovedChangeModel struct {
	StateChange
}

// ThresholdChangeModel maps to GraphQL ThresholdChange: SIGNATURE_THRESHOLD × UPDATE.
type ThresholdChangeModel struct {
	StateChange
}

// AccountFlagsChangeModel maps to GraphQL AccountFlagsChange: FLAGS × SET/CLEAR.
type AccountFlagsChangeModel struct {
	StateChange
}

// HomeDomainSetChangeModel maps to GraphQL HomeDomainSetChange: HOME_DOMAIN × SET.
type HomeDomainSetChangeModel struct {
	StateChange
}

// HomeDomainUpdatedChangeModel maps to GraphQL HomeDomainUpdatedChange: HOME_DOMAIN × UPDATE.
type HomeDomainUpdatedChangeModel struct {
	StateChange
}

// HomeDomainClearedChangeModel maps to GraphQL HomeDomainClearedChange: HOME_DOMAIN × CLEAR.
type HomeDomainClearedChangeModel struct {
	StateChange
}

// DataEntryAddedChangeModel maps to GraphQL DataEntryAddedChange: DATA_ENTRY × ADD.
type DataEntryAddedChangeModel struct {
	StateChange
}

// DataEntryUpdatedChangeModel maps to GraphQL DataEntryUpdatedChange: DATA_ENTRY × UPDATE.
type DataEntryUpdatedChangeModel struct {
	StateChange
}

// DataEntryRemovedChangeModel maps to GraphQL DataEntryRemovedChange: DATA_ENTRY × REMOVE.
type DataEntryRemovedChangeModel struct {
	StateChange
}

// AllowanceChangeModel maps to GraphQL AllowanceChange: ALLOWANCE × UPDATE (SEP-41 approve).
type AllowanceChangeModel struct {
	StateChange
}

// TrustlineAddedChangeModel maps to GraphQL TrustlineAddedChange: TRUSTLINE × ADD.
type TrustlineAddedChangeModel struct {
	StateChange
}

// TrustlineUpdatedChangeModel maps to GraphQL TrustlineUpdatedChange: TRUSTLINE × UPDATE.
type TrustlineUpdatedChangeModel struct {
	StateChange
}

// TrustlineRemovedChangeModel maps to GraphQL TrustlineRemovedChange: TRUSTLINE × REMOVE.
type TrustlineRemovedChangeModel struct {
	StateChange
}

// BalanceAuthorizationChangeModel maps to GraphQL BalanceAuthorizationChange: BALANCE_AUTHORIZATION × SET/CLEAR.
type BalanceAuthorizationChangeModel struct {
	StateChange
}
