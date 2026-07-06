package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// OperationType represents the type of Stellar operation
type OperationType string

const (
	OperationTypeCreateAccount                 OperationType = "CREATE_ACCOUNT"
	OperationTypePayment                       OperationType = "PAYMENT"
	OperationTypePathPaymentStrictReceive      OperationType = "PATH_PAYMENT_STRICT_RECEIVE"
	OperationTypePathPaymentStrictSend         OperationType = "PATH_PAYMENT_STRICT_SEND"
	OperationTypeManageSellOffer               OperationType = "MANAGE_SELL_OFFER"
	OperationTypeCreatePassiveSellOffer        OperationType = "CREATE_PASSIVE_SELL_OFFER"
	OperationTypeManageBuyOffer                OperationType = "MANAGE_BUY_OFFER"
	OperationTypeSetOptions                    OperationType = "SET_OPTIONS"
	OperationTypeChangeTrust                   OperationType = "CHANGE_TRUST"
	OperationTypeAllowTrust                    OperationType = "ALLOW_TRUST"
	OperationTypeAccountMerge                  OperationType = "ACCOUNT_MERGE"
	OperationTypeInflation                     OperationType = "INFLATION"
	OperationTypeManageData                    OperationType = "MANAGE_DATA"
	OperationTypeBumpSequence                  OperationType = "BUMP_SEQUENCE"
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

// StateChangeCategory represents the category of state change
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
)

// StateChangeReason represents the reason for a state change
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

// TokenType represents the type of token/balance
type TokenType string

const (
	TokenTypeNative        TokenType = "NATIVE"
	TokenTypeClassic       TokenType = "CLASSIC"
	TokenTypeSAC           TokenType = "SAC"
	TokenTypeSEP41         TokenType = "SEP41"
	TokenTypeLiquidityPool TokenType = "LIQUIDITY_POOL"
)

// Balance is an interface representing different types of account balances
type Balance interface {
	GetBalance() string
	GetTokenID() string
	GetTokenType() TokenType
	isBalance()
}

// NativeBalance represents a native XLM balance
type NativeBalance struct {
	BalanceValue string    `json:"balance"`
	TokenID      string    `json:"tokenId"`
	TokenType    TokenType `json:"tokenType"`
	// MinimumBalance is the base reserve requirement (excludes liabilities):
	// (2 + numSubentries + numSponsoring - numSponsored) * baseReserve; matches stellar-core getMinBalance.
	// Spendable balance = balance - MinimumBalance - SellingLiabilities.
	MinimumBalance     string `json:"minimumBalance"`
	BuyingLiabilities  string `json:"buyingLiabilities"`
	SellingLiabilities string `json:"sellingLiabilities"`
	LastModifiedLedger uint32 `json:"lastModifiedLedger"`
	NumSubentries      uint32 `json:"numSubentries"`
}

func (b *NativeBalance) GetBalance() string      { return b.BalanceValue }
func (b *NativeBalance) GetTokenID() string      { return b.TokenID }
func (b *NativeBalance) GetTokenType() TokenType { return b.TokenType }
func (b *NativeBalance) isBalance()              {}

// TrustlineBalance represents a classic Stellar asset trustline balance
type TrustlineBalance struct {
	BalanceValue                      string    `json:"balance"`
	TokenID                           string    `json:"tokenId"`
	TokenType                         TokenType `json:"tokenType"`
	Code                              *string   `json:"code,omitempty"`
	Issuer                            *string   `json:"issuer,omitempty"`
	Type                              string    `json:"type"`
	Limit                             string    `json:"limit"`
	BuyingLiabilities                 string    `json:"buyingLiabilities"`
	SellingLiabilities                string    `json:"sellingLiabilities"`
	LastModifiedLedger                uint32    `json:"lastModifiedLedger"`
	IsAuthorized                      bool      `json:"isAuthorized"`
	IsAuthorizedToMaintainLiabilities bool      `json:"isAuthorizedToMaintainLiabilities"`
}

func (b *TrustlineBalance) GetBalance() string      { return b.BalanceValue }
func (b *TrustlineBalance) GetTokenID() string      { return b.TokenID }
func (b *TrustlineBalance) GetTokenType() TokenType { return b.TokenType }
func (b *TrustlineBalance) isBalance()              {}

// SACBalance represents a Stellar Asset Contract (Soroban) balance
type SACBalance struct {
	BalanceValue      string    `json:"balance"`
	TokenID           string    `json:"tokenId"`
	TokenType         TokenType `json:"tokenType"`
	Code              string    `json:"code"`
	Issuer            string    `json:"issuer"`
	Decimals          int32     `json:"decimals"`
	IsAuthorized      bool      `json:"isAuthorized"`
	IsClawbackEnabled bool      `json:"isClawbackEnabled"`
}

func (b *SACBalance) GetBalance() string      { return b.BalanceValue }
func (b *SACBalance) GetTokenID() string      { return b.TokenID }
func (b *SACBalance) GetTokenType() TokenType { return b.TokenType }
func (b *SACBalance) isBalance()              {}

// SEP41Balance represents a pure SEP-41 (non-SAC) token contract balance.
// BalanceValue is the raw i128 amount as a decimal string (e.g. "5000000000"),
// not scaled by Decimals — i128 amounts can exceed int64, so the server does not
// pre-format them the way it does for native/classic/SAC balances.
type SEP41Balance struct {
	BalanceValue       string    `json:"balance"`
	TokenID            string    `json:"tokenId"`
	TokenType          TokenType `json:"tokenType"`
	Name               *string   `json:"name,omitempty"`
	Symbol             *string   `json:"symbol,omitempty"`
	Decimals           int32     `json:"decimals"`
	LastModifiedLedger uint32    `json:"lastModifiedLedger"`
}

func (b *SEP41Balance) GetBalance() string      { return b.BalanceValue }
func (b *SEP41Balance) GetTokenID() string      { return b.TokenID }
func (b *SEP41Balance) GetTokenType() TokenType { return b.TokenType }
func (b *SEP41Balance) isBalance()              {}

// LiquidityPoolReserve is one constituent asset of a liquidity pool and its reserve amount.
type LiquidityPoolReserve struct {
	Asset  string `json:"asset"`
	Amount string `json:"amount"`
}

// LiquidityPoolBalance represents an account's liquidity-pool share holding.
// BalanceValue is the account's pool shares and TokenID is the pool id; Reserves carries
// the pool's constituent assets and amounts.
type LiquidityPoolBalance struct {
	BalanceValue       string                 `json:"balance"`
	TokenID            string                 `json:"tokenId"`
	TokenType          TokenType              `json:"tokenType"`
	LiquidityPoolID    string                 `json:"liquidityPoolId"`
	Reserves           []LiquidityPoolReserve `json:"reserves"`
	LastModifiedLedger uint32                 `json:"lastModifiedLedger"`
}

func (b *LiquidityPoolBalance) GetBalance() string      { return b.BalanceValue }
func (b *LiquidityPoolBalance) GetTokenID() string      { return b.TokenID }
func (b *LiquidityPoolBalance) GetTokenType() TokenType { return b.TokenType }
func (b *LiquidityPoolBalance) isBalance()              {}

// UnmarshalBalance unmarshals a JSON balance into the appropriate concrete type
// based on the __typename field
func UnmarshalBalance(data []byte) (Balance, error) {
	// First, peek at the __typename field
	var typeInfo struct {
		TypeName string `json:"__typename"`
	}

	if err := json.Unmarshal(data, &typeInfo); err != nil {
		return nil, fmt.Errorf("unmarshaling balance type: %w", err)
	}

	// Unmarshal into the appropriate concrete type based on __typename
	switch typeInfo.TypeName {
	case "NativeBalance":
		var balance NativeBalance
		if err := json.Unmarshal(data, &balance); err != nil {
			return nil, fmt.Errorf("unmarshaling native balance: %w", err)
		}
		return &balance, nil
	case "TrustlineBalance":
		var balance TrustlineBalance
		if err := json.Unmarshal(data, &balance); err != nil {
			return nil, fmt.Errorf("unmarshaling trustline balance: %w", err)
		}
		return &balance, nil
	case "SACBalance":
		var balance SACBalance
		if err := json.Unmarshal(data, &balance); err != nil {
			return nil, fmt.Errorf("unmarshaling SAC balance: %w", err)
		}
		return &balance, nil
	case "SEP41Balance":
		var balance SEP41Balance
		if err := json.Unmarshal(data, &balance); err != nil {
			return nil, fmt.Errorf("unmarshaling SEP-41 balance: %w", err)
		}
		return &balance, nil
	case "LiquidityPoolBalance":
		var balance LiquidityPoolBalance
		if err := json.Unmarshal(data, &balance); err != nil {
			return nil, fmt.Errorf("unmarshaling liquidity pool balance: %w", err)
		}
		return &balance, nil
	default:
		return nil, fmt.Errorf("unknown balance type: %s", typeInfo.TypeName)
	}
}

// Account represents a Stellar account
type Account struct {
	Address   string    `json:"address"`
	CreatedAt time.Time `json:"createdAt,omitempty"`
}

// Transaction represents a Stellar transaction XDR for submission.
type Transaction struct {
	TransactionXdr string `json:"transactionXdr" validate:"required"`
}

// GraphQLTransaction represents a transaction from the GraphQL API
type GraphQLTransaction struct {
	Hash            string    `json:"hash"`
	FeeCharged      int64     `json:"feeCharged"`
	ResultCode      string    `json:"resultCode"`
	LedgerNumber    uint32    `json:"ledgerNumber"`
	LedgerCreatedAt time.Time `json:"ledgerCreatedAt"`
	IsFeeBump       bool      `json:"isFeeBump"`
	IngestedAt      time.Time `json:"ingestedAt"`
}

// Operation represents a Stellar operation
type Operation struct {
	ID              int64         `json:"id"`
	OperationType   OperationType `json:"operationType"`
	OperationXdr    string        `json:"operationXdr"`
	ResultCode      string        `json:"resultCode"`
	Successful      bool          `json:"successful"`
	LedgerNumber    uint32        `json:"ledgerNumber"`
	LedgerCreatedAt time.Time     `json:"ledgerCreatedAt"`
	IngestedAt      time.Time     `json:"ingestedAt"`
}

// PageInfo contains pagination information
type PageInfo struct {
	StartCursor     *string `json:"startCursor,omitempty"`
	EndCursor       *string `json:"endCursor,omitempty"`
	HasNextPage     bool    `json:"hasNextPage"`
	HasPreviousPage bool    `json:"hasPreviousPage"`
}

// TransactionEdge represents an edge in the transaction connection
type TransactionEdge struct {
	Node   *GraphQLTransaction `json:"node,omitempty"`
	Cursor string              `json:"cursor"`
}

// UnmarshalJSON implements custom JSON unmarshaling for TransactionEdge.
// The GraphQL schema declares the edge as `node: Transaction` (nullable),
// so a null or missing node is a schema-valid response and leaves
// e.Node == nil rather than returning an error. Callers iterating
// connection.Edges must nil-check edge.Node.
func (e *TransactionEdge) UnmarshalJSON(data []byte) error {
	type tempEdge struct {
		Node   json.RawMessage `json:"node"`
		Cursor string          `json:"cursor"`
	}

	var temp tempEdge
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("unmarshaling transaction edge: %w", err)
	}

	e.Cursor = temp.Cursor

	if len(temp.Node) == 0 || string(temp.Node) == "null" {
		e.Node = nil
		return nil
	}

	var node GraphQLTransaction
	if err := json.Unmarshal(temp.Node, &node); err != nil {
		return fmt.Errorf("decoding transaction edge node: %w", err)
	}
	e.Node = &node
	return nil
}

// unmarshalConnection decodes a Relay-style connection payload and enforces the schema's non-null
// guarantees shared by every *Connection type: a null edge entry and a missing/null pageInfo are
// always rejected, and a missing/null edges field is rejected when requireEdges is set (some schema
// connections declare edges nullable). connName and edgeTypeName are woven into the error messages.
// Per-edge decoding (node-null guards, polymorphic nodes) stays with each edge type's own
// UnmarshalJSON, which json.Unmarshal invokes while decoding []*E.
func unmarshalConnection[E any](data []byte, connName, edgeTypeName string, requireEdges bool) ([]*E, *PageInfo, error) {
	type tempConnection struct {
		Edges    []*E      `json:"edges"`
		PageInfo *PageInfo `json:"pageInfo"`
	}
	var temp tempConnection
	if err := json.Unmarshal(data, &temp); err != nil {
		return nil, nil, fmt.Errorf("unmarshaling %s: %w", connName, err)
	}
	if requireEdges && temp.Edges == nil {
		return nil, nil, fmt.Errorf("%s missing required edges field: the GraphQL schema declares edges as non-null", connName)
	}
	for i, edge := range temp.Edges {
		if edge == nil {
			return nil, nil, fmt.Errorf("%s edge at index %d is null: the GraphQL schema declares %s as non-null", connName, i, edgeTypeName)
		}
	}
	if temp.PageInfo == nil {
		return nil, nil, fmt.Errorf("%s missing required pageInfo field: the GraphQL schema declares pageInfo as non-null", connName)
	}
	return temp.Edges, temp.PageInfo, nil
}

// TransactionConnection represents a paginated list of transactions
type TransactionConnection struct {
	Edges    []*TransactionEdge `json:"edges,omitempty"`
	PageInfo *PageInfo          `json:"pageInfo"`
}

// UnmarshalJSON implements custom JSON unmarshaling for TransactionConnection
// and enforces the schema's non-null guarantees. The schema declares
// edges as [TransactionEdge!] and pageInfo as PageInfo!, so:
//   - a null entry within the edges array is a server bug and is rejected
//   - a missing or null pageInfo field is a server bug and is rejected
//
// In contrast to BalanceConnection, the edges field itself is nullable in
// the schema, so a missing or null edges field is accepted (Edges stays nil).
func (c *TransactionConnection) UnmarshalJSON(data []byte) error {
	edges, pageInfo, err := unmarshalConnection[TransactionEdge](data, "transaction connection", "TransactionEdge", false)
	if err != nil {
		return err
	}
	c.Edges = edges
	c.PageInfo = pageInfo
	return nil
}

// OperationEdge represents an edge in the operation connection
type OperationEdge struct {
	Node   *Operation `json:"node,omitempty"`
	Cursor string     `json:"cursor"`
}

// UnmarshalJSON implements custom JSON unmarshaling for OperationEdge.
// The GraphQL schema declares the edge as `node: Operation` (nullable),
// so a null or missing node is a schema-valid response and leaves
// e.Node == nil rather than returning an error. Callers iterating
// connection.Edges must nil-check edge.Node.
func (e *OperationEdge) UnmarshalJSON(data []byte) error {
	type tempEdge struct {
		Node   json.RawMessage `json:"node"`
		Cursor string          `json:"cursor"`
	}

	var temp tempEdge
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("unmarshaling operation edge: %w", err)
	}

	e.Cursor = temp.Cursor

	if len(temp.Node) == 0 || string(temp.Node) == "null" {
		e.Node = nil
		return nil
	}

	var node Operation
	if err := json.Unmarshal(temp.Node, &node); err != nil {
		return fmt.Errorf("decoding operation edge node: %w", err)
	}
	e.Node = &node
	return nil
}

// OperationConnection represents a paginated list of operations
type OperationConnection struct {
	Edges    []*OperationEdge `json:"edges,omitempty"`
	PageInfo *PageInfo        `json:"pageInfo"`
}

// UnmarshalJSON implements custom JSON unmarshaling for OperationConnection
// and enforces the schema's non-null guarantees. The schema declares
// edges as [OperationEdge!] and pageInfo as PageInfo!, so:
//   - a null entry within the edges array is a server bug and is rejected
//   - a missing or null pageInfo field is a server bug and is rejected
//
// In contrast to BalanceConnection, the edges field itself is nullable in
// the schema, so a missing or null edges field is accepted (Edges stays nil).
func (c *OperationConnection) UnmarshalJSON(data []byte) error {
	edges, pageInfo, err := unmarshalConnection[OperationEdge](data, "operation connection", "OperationEdge", false)
	if err != nil {
		return err
	}
	c.Edges = edges
	c.PageInfo = pageInfo
	return nil
}

// StateChangeEdge represents an edge in the state change connection
type StateChangeEdge struct {
	Node   StateChangeNode `json:"node,omitempty"`
	Cursor string          `json:"cursor"`
}

// UnmarshalJSON implements custom JSON unmarshaling for StateChangeEdge.
// The GraphQL schema declares the edge as `node: BaseStateChange`
// (nullable), so a null or missing node is a schema-valid response and
// leaves e.Node == nil. When a node is present, it is dispatched to the
// correct concrete type via UnmarshalStateChangeNode (which reads the
// __typename discriminator).
func (e *StateChangeEdge) UnmarshalJSON(data []byte) error {
	// Create a temporary struct to unmarshal the edge structure
	type tempEdge struct {
		Node   json.RawMessage `json:"node"`
		Cursor string          `json:"cursor"`
	}

	var temp tempEdge
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("unmarshaling state change edge: %w", err)
	}

	e.Cursor = temp.Cursor

	// If node is null, return early
	if len(temp.Node) == 0 || string(temp.Node) == "null" {
		e.Node = nil
		return nil
	}

	// Unmarshal the node using the polymorphic unmarshaler
	node, err := UnmarshalStateChangeNode(temp.Node)
	if err != nil {
		return err
	}

	e.Node = node
	return nil
}

// StateChangeConnection represents a paginated list of state changes
type StateChangeConnection struct {
	Edges    []*StateChangeEdge `json:"edges,omitempty"`
	PageInfo *PageInfo          `json:"pageInfo"`
}

// UnmarshalJSON implements custom JSON unmarshaling for StateChangeConnection
// and enforces the schema's non-null guarantees. The schema declares
// edges as [StateChangeEdge!] and pageInfo as PageInfo!, so:
//   - a null entry within the edges array is a server bug and is rejected
//   - a missing or null pageInfo field is a server bug and is rejected
//
// In contrast to BalanceConnection, the edges field itself is nullable in
// the schema, so a missing or null edges field is accepted (Edges stays nil).
func (c *StateChangeConnection) UnmarshalJSON(data []byte) error {
	edges, pageInfo, err := unmarshalConnection[StateChangeEdge](data, "state change connection", "StateChangeEdge", false)
	if err != nil {
		return err
	}
	c.Edges = edges
	c.PageInfo = pageInfo
	return nil
}

// BalanceEdge represents an edge in the balance connection
type BalanceEdge struct {
	Node   Balance `json:"node,omitempty"`
	Cursor string  `json:"cursor"`
}

// UnmarshalJSON implements custom JSON unmarshaling for BalanceEdge
// to properly handle polymorphic balance types (NativeBalance,
// TrustlineBalance, SACBalance) discriminated by __typename.
func (e *BalanceEdge) UnmarshalJSON(data []byte) error {
	type tempEdge struct {
		Node   json.RawMessage `json:"node"`
		Cursor string          `json:"cursor"`
	}

	var temp tempEdge
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("unmarshaling balance edge: %w", err)
	}

	e.Cursor = temp.Cursor

	if len(temp.Node) == 0 || string(temp.Node) == "null" {
		return fmt.Errorf("balance edge missing required node (cursor=%q): the GraphQL schema declares Balance as non-null", temp.Cursor)
	}

	node, err := UnmarshalBalance(temp.Node)
	if err != nil {
		return err
	}

	e.Node = node
	return nil
}

// BalanceConnection represents a paginated list of balances
type BalanceConnection struct {
	Edges    []*BalanceEdge `json:"edges,omitempty"`
	PageInfo *PageInfo      `json:"pageInfo"`
}

// UnmarshalJSON implements custom JSON unmarshaling for BalanceConnection
// and enforces the schema's non-null guarantees. The GraphQL schema declares
// edges as [BalanceEdge!]! and pageInfo as PageInfo!, so each of the
// following is a server bug and is rejected here:
//   - a missing or null edges field on the connection
//   - a null entry within the edges array
//   - a missing or null pageInfo field on the connection
//
// Null nodes inside an edge object are caught separately by
// BalanceEdge.UnmarshalJSON.
func (c *BalanceConnection) UnmarshalJSON(data []byte) error {
	edges, pageInfo, err := unmarshalConnection[BalanceEdge](data, "balance connection", "BalanceEdge", true)
	if err != nil {
		return err
	}
	c.Edges = edges
	c.PageInfo = pageInfo
	return nil
}

// AccountTransactionConnection is an account's transactions with that account's per-transaction
// operations and state changes embedded on each edge.
type AccountTransactionConnection struct {
	Edges    []*AccountTransactionEdge `json:"edges,omitempty"`
	PageInfo *PageInfo                 `json:"pageInfo"`
}

// AccountTransactionEdge is one transaction plus the calling account's operations and state
// changes within it. Operations and StateChanges are always populated (possibly empty).
type AccountTransactionEdge struct {
	Node         *GraphQLTransaction `json:"node"`
	Cursor       string              `json:"cursor"`
	Operations   []*Operation        `json:"operations"`
	StateChanges []StateChangeNode   `json:"stateChanges"`
}

// UnmarshalJSON decodes the edge, dispatching each state-change node by its __typename via
// UnmarshalStateChangeNode. The GraphQL schema declares node as Transaction!, operations as
// [Operation!]!, and stateChanges as [BaseStateChange!]! — all non-null — so a missing/null node, a
// null operations or stateChanges list, or a null element within either is a server bug and is
// rejected.
func (e *AccountTransactionEdge) UnmarshalJSON(dataBytes []byte) error {
	type tempEdge struct {
		Node         json.RawMessage   `json:"node"`
		Cursor       string            `json:"cursor"`
		Operations   []*Operation      `json:"operations"`
		StateChanges []json.RawMessage `json:"stateChanges"`
	}
	var temp tempEdge
	if err := json.Unmarshal(dataBytes, &temp); err != nil {
		return fmt.Errorf("unmarshaling detailed transaction edge: %w", err)
	}
	e.Cursor = temp.Cursor

	if len(temp.Node) == 0 || string(temp.Node) == "null" {
		return fmt.Errorf("detailed transaction edge missing required node: the GraphQL schema declares Transaction as non-null")
	}
	var node GraphQLTransaction
	if err := json.Unmarshal(temp.Node, &node); err != nil {
		return fmt.Errorf("decoding detailed transaction edge node: %w", err)
	}
	e.Node = &node

	if temp.Operations == nil {
		return fmt.Errorf("detailed transaction edge missing required operations: the GraphQL schema declares [Operation!]! as non-null")
	}
	for i, op := range temp.Operations {
		if op == nil {
			return fmt.Errorf("detailed transaction edge operation at index %d is null: the GraphQL schema declares Operation as non-null", i)
		}
	}
	e.Operations = temp.Operations

	if temp.StateChanges == nil {
		return fmt.Errorf("detailed transaction edge missing required stateChanges: the GraphQL schema declares [BaseStateChange!]! as non-null")
	}
	e.StateChanges = make([]StateChangeNode, 0, len(temp.StateChanges))
	for i, raw := range temp.StateChanges {
		if len(raw) == 0 || string(raw) == "null" {
			return fmt.Errorf("detailed transaction edge state change at index %d is null: the GraphQL schema declares BaseStateChange as non-null", i)
		}
		sc, err := UnmarshalStateChangeNode(raw)
		if err != nil {
			return fmt.Errorf("decoding detailed transaction edge state change: %w", err)
		}
		e.StateChanges = append(e.StateChanges, sc)
	}
	return nil
}

// UnmarshalJSON enforces the schema's non-null guarantees, matching BalanceConnection. The
// schema declares edges as [AccountTransactionEdge!]! and pageInfo as PageInfo!, so each of
// the following is a server bug and is rejected here:
//   - a missing or null edges field on the connection
//   - a null entry within the edges array
//   - a missing or null pageInfo field on the connection
//
// Null nodes inside an edge object are caught separately by AccountTransactionEdge.UnmarshalJSON.
func (c *AccountTransactionConnection) UnmarshalJSON(dataBytes []byte) error {
	edges, pageInfo, err := unmarshalConnection[AccountTransactionEdge](dataBytes, "detailed transaction connection", "AccountTransactionEdge", true)
	if err != nil {
		return err
	}
	c.Edges = edges
	c.PageInfo = pageInfo
	return nil
}

// Balances returns the connection's balance nodes as a flat slice.
// Returns nil if the receiver is nil or has no edges. Combined with the
// strict UnmarshalJSON on BalanceEdge and BalanceConnection, callers
// using this helper can trust that every returned Balance is non-nil.
//
// The defensive nil-edge skip protects against connections constructed
// directly in Go code (not via JSON decode); JSON-derived connections
// never reach this branch because UnmarshalJSON rejects null edges.
func (c *BalanceConnection) Balances() []Balance {
	if c == nil || len(c.Edges) == 0 {
		return nil
	}
	balances := make([]Balance, 0, len(c.Edges))
	for _, edge := range c.Edges {
		if edge == nil {
			continue
		}
		balances = append(balances, edge.Node)
	}
	return balances
}
