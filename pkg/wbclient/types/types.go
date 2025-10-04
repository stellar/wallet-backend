package types

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/stellar/wallet-backend/internal/entities"
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

// Account represents a Stellar account
type Account struct {
	Address   string    `json:"address"`
	CreatedAt time.Time `json:"createdAt,omitempty"`
}

// Transaction represents a Stellar transaction
type Transaction struct {
	TransactionXdr   string                                `json:"transactionXdr" validate:"required"`
	SimulationResult entities.RPCSimulateTransactionResult `json:"simulationResult,omitempty"`
}

// GraphQLTransaction represents a transaction from the GraphQL API
type GraphQLTransaction struct {
	Hash            string    `json:"hash"`
	EnvelopeXdr     string    `json:"envelopeXdr"`
	ResultXdr       string    `json:"resultXdr"`
	MetaXdr         string    `json:"metaXdr"`
	LedgerNumber    uint32    `json:"ledgerNumber"`
	LedgerCreatedAt time.Time `json:"ledgerCreatedAt"`
	IngestedAt      time.Time `json:"ingestedAt"`
}

// Operation represents a Stellar operation
type Operation struct {
	ID              int64         `json:"id"`
	OperationType   OperationType `json:"operationType"`
	OperationXdr    string        `json:"operationXdr"`
	LedgerNumber    uint32        `json:"ledgerNumber"`
	LedgerCreatedAt time.Time     `json:"ledgerCreatedAt"`
	IngestedAt      time.Time     `json:"ingestedAt"`
}

// StateChange represents a blockchain state change with all possible fields
type StateChange struct {
	Type            StateChangeCategory `json:"type"`
	Reason          StateChangeReason   `json:"reason"`
	IngestedAt      time.Time           `json:"ingestedAt"`
	LedgerCreatedAt time.Time           `json:"ledgerCreatedAt"`
	LedgerNumber    uint32              `json:"ledgerNumber"`

	// Fields for balance changes
	TokenID *string `json:"tokenId,omitempty"`
	Amount  *string `json:"amount,omitempty"`

	// Fields for signer changes
	SignerAddress *string `json:"signerAddress,omitempty"`
	SignerWeights *string `json:"signerWeights,omitempty"`

	// Fields for threshold changes
	Thresholds *string `json:"thresholds,omitempty"`

	// Fields for metadata changes
	KeyValue *string `json:"keyValue,omitempty"`

	// Fields for flags changes
	Flags []string `json:"flags,omitempty"`

	// Fields for trustline changes
	Limit *string `json:"limit,omitempty"`

	// Fields for reserves changes
	SponsoredAddress *string `json:"sponsoredAddress,omitempty"`
	SponsorAddress   *string `json:"sponsorAddress,omitempty"`
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

// TransactionConnection represents a paginated list of transactions
type TransactionConnection struct {
	Edges    []*TransactionEdge `json:"edges,omitempty"`
	PageInfo *PageInfo          `json:"pageInfo"`
}

// OperationEdge represents an edge in the operation connection
type OperationEdge struct {
	Node   *Operation `json:"node,omitempty"`
	Cursor string     `json:"cursor"`
}

// OperationConnection represents a paginated list of operations
type OperationConnection struct {
	Edges    []*OperationEdge `json:"edges,omitempty"`
	PageInfo *PageInfo        `json:"pageInfo"`
}

// StateChangeEdge represents an edge in the state change connection
type StateChangeEdge struct {
	Node   StateChangeNode `json:"node,omitempty"`
	Cursor string          `json:"cursor"`
}

// UnmarshalJSON implements custom JSON unmarshaling for StateChangeEdge
// to properly handle polymorphic state change types
func (e *StateChangeEdge) UnmarshalJSON(data []byte) error {
	// Create a temporary struct to unmarshal the edge structure
	type tempEdge struct {
		Node   json.RawMessage `json:"node,omitempty"`
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

// RegisterAccountInput is the input for registering an account
type RegisterAccountInput struct {
	Address string `json:"address"`
}

// RegisterAccountPayload is the response for registering an account
type RegisterAccountPayload struct {
	Success bool     `json:"success"`
	Account *Account `json:"account,omitempty"`
}

// DeregisterAccountInput is the input for deregistering an account
type DeregisterAccountInput struct {
	Address string `json:"address"`
}

// DeregisterAccountPayload is the response for deregistering an account
type DeregisterAccountPayload struct {
	Success bool    `json:"success"`
	Message *string `json:"message,omitempty"`
}

type BuildTransactionsRequest struct {
	Transactions []Transaction `json:"transactions" validate:"required,gt=0"`
}

type BuildTransactionsResponse struct {
	TransactionXDRs []string `json:"transactionXdrs"`
}

type BuildTransactionResponse struct {
	TransactionXDR string `json:"transactionXdr"`
}

type CreateFeeBumpTransactionRequest struct {
	Transaction string `json:"transaction" validate:"required"`
}

type TransactionEnvelopeResponse struct {
	Transaction       string `json:"transaction"`
	NetworkPassphrase string `json:"networkPassphrase"`
}
