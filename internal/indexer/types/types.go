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
	StellarAddress string    `json:"stellarAddress,omitempty" db:"stellar_address"`
	CreatedAt      time.Time `json:"createdAt,omitempty" db:"created_at"`
}

type Transaction struct {
	Hash            string    `json:"hash,omitempty" db:"hash"`
	ToID            int64     `json:"to_id,omitempty" db:"to_id"`
	EnvelopeXDR     string    `json:"envelopeXdr,omitempty" db:"envelope_xdr"`
	ResultXDR       string    `json:"resultXdr,omitempty" db:"result_xdr"`
	MetaXDR         string    `json:"metaXdr,omitempty" db:"meta_xdr"`
	LedgerNumber    uint32    `json:"ledgerNumber,omitempty" db:"ledger_number"`
	LedgerCreatedAt time.Time `json:"ledgerCreatedAt,omitempty" db:"ledger_created_at"`
	IngestedAt      time.Time `json:"ingestedAt,omitempty" db:"ingested_at"`
	// Relationships:
	Operations   []Operation   `json:"operations,omitempty" db:"operations"`
	Accounts     []Account     `json:"accounts,omitempty" db:"accounts"`
	StateChanges []StateChange `json:"stateChanges,omitempty" db:"state_changes"`
}

type TransactionWithAccountID struct {
	Transaction
	AccountID string `json:"accountId,omitempty" db:"account_id"`
}

type AccountWithTxHash struct {
	Account
	TxHash string `json:"txHash,omitempty" db:"tx_hash"`
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
	LedgerCreatedAt time.Time     `json:"ledgerCreatedAt,omitempty" db:"ledger_created_at"`
	IngestedAt      time.Time     `json:"ingestedAt,omitempty" db:"ingested_at"`
	// Relationships:
	TxHash       string        `json:"txHash,omitempty" db:"tx_hash"`
	Transaction  *Transaction  `json:"transaction,omitempty" db:"transaction"`
	Accounts     []Account     `json:"accounts,omitempty" db:"accounts"`
	StateChanges []StateChange `json:"stateChanges,omitempty" db:"state_changes"`
}

type OperationWithAccountID struct {
	Operation
	AccountID string `db:"account_id"`
}

type StateChangeCategory string

const (
	StateChangeCategoryDebit              StateChangeCategory = "DEBIT"
	StateChangeCategoryCredit             StateChangeCategory = "CREDIT"
	StateChangeCategoryMint               StateChangeCategory = "MINT"
	StateChangeCategoryBurn               StateChangeCategory = "BURN"
	StateChangeCategorySigner             StateChangeCategory = "SIGNER"
	StateChangeCategorySignatureThreshold StateChangeCategory = "SIGNATURE_THRESHOLD"
	StateChangeCategoryMetadata           StateChangeCategory = "METADATA"
	StateChangeCategoryFlags              StateChangeCategory = "FLAGS"
	StateChangeCategoryLiability          StateChangeCategory = "LIABILITY"
	StateChangeCategoryTrustlineFlags     StateChangeCategory = "TRUSTLINE_FLAGS"
	StateChangeCategorySponsorship        StateChangeCategory = "SPONSORSHIP"
	StateChangeCategoryUnsupported        StateChangeCategory = "UNSUPPORTED"
	StateChangeCategoryAllowance          StateChangeCategory = "ALLOWANCE"
	StateChangeCategoryContract           StateChangeCategory = "CONTRACT"
	StateChangeCategoryAuthorization      StateChangeCategory = "AUTHORIZATION"
)

type StateChangeReason string

const (
	StateChangeReasonAdd        StateChangeReason = "ADD"
	StateChangeReasonRemove     StateChangeReason = "REMOVE"
	StateChangeReasonUpdate     StateChangeReason = "UPDATE"
	StateChangeReasonLow        StateChangeReason = "LOW"
	StateChangeReasonMedium     StateChangeReason = "MEDIUM"
	StateChangeReasonHigh       StateChangeReason = "HIGH"
	StateChangeReasonHomeDomain StateChangeReason = "HOME_DOMAIN"
	StateChangeReasonSet        StateChangeReason = "SET"
	StateChangeReasonClear      StateChangeReason = "CLEAR"
	StateChangeReasonSell       StateChangeReason = "SELL"
	StateChangeReasonBuy        StateChangeReason = "BUY"
	StateChangeReasonDataEntry  StateChangeReason = "DATA_ENTRY"
	StateChangeReasonConsume    StateChangeReason = "CONSUME"
	StateChangeReasonDeploy     StateChangeReason = "DEPLOY"
	StateChangeReasonInvoke     StateChangeReason = "INVOKE"
)

type StateChange struct {
	ID                  string              `json:"id,omitempty" db:"id"`
	StateChangeCategory StateChangeCategory `json:"stateChangeCategory,omitempty" db:"state_change_category"`
	StateChangeReason   *StateChangeReason  `json:"stateChangeReason,omitempty" db:"state_change_reason"`
	IngestedAt          time.Time           `json:"ingestedAt,omitempty" db:"ingested_at"`
	LedgerCreatedAt     time.Time           `json:"ledgerCreatedAt,omitempty" db:"ledger_created_at"`
	LedgerNumber        uint32              `json:"ledgerNumber,omitempty" db:"ledger_number"`
	// Nullable fields:
	TokenID            sql.NullString `json:"tokenId,omitempty" db:"token_id"`
	Amount             sql.NullString `json:"amount,omitempty" db:"amount"`
	ClaimableBalanceID sql.NullString `json:"claimableBalanceId,omitempty" db:"claimable_balance_id"`
	LiquidityPoolID    sql.NullString `json:"liquidityPoolId,omitempty" db:"liquidity_pool_id"`
	OfferID            sql.NullString `json:"offerId,omitempty" db:"offer_id"`
	SignerAccountID    sql.NullString `json:"signerAccountId,omitempty" db:"signer_account_id"`
	SpenderAccountID   sql.NullString `json:"spenderAccountId,omitempty" db:"spender_account_id"`
	SponsoredAccountID sql.NullString `json:"sponsoredAccountId,omitempty" db:"sponsored_account_id"`
	SponsorAccountID   sql.NullString `json:"sponsorAccountId,omitempty" db:"sponsor_account_id"`
	// Nullable JSONB fields: // TODO: update from `NullableJSONB` to custom objects, except for KeyValue.
	SignerWeights NullableJSONB `json:"signerWeights,omitempty" db:"signer_weights"`
	Thresholds    NullableJSONB `json:"thresholds,omitempty" db:"thresholds"`
	Flags         NullableJSON  `json:"flags,omitempty" db:"flags"`
	KeyValue      NullableJSONB `json:"keyValue,omitempty" db:"key_value"`
	// Relationships:
	AccountID   string       `json:"accountId,omitempty" db:"account_id"`
	Account     *Account     `json:"account,omitempty" db:"account"`
	OperationID int64        `json:"operationId,omitempty" db:"operation_id"`
	Operation   *Operation   `json:"operation,omitempty" db:"operation"`
	TxHash      string       `json:"txHash,omitempty" db:"tx_hash"`
	Transaction *Transaction `json:"transaction,omitempty" db:"transaction"`
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
