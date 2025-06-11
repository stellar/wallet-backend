package types

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"time"
)

type Account struct {
	StellarAddress string    `json:"stellarAddress,omitempty" db:"stellar_address"`
	CreatedAt      time.Time `json:"createdAt,omitempty" db:"created_at"`
}

type Transaction struct {
	Hash              string    `json:"hash,omitempty" db:"hash"`
	EnvelopeXDR       string    `json:"envelopeXdr,omitempty" db:"envelope_xdr"`
	ResultXDR         string    `json:"resultXdr,omitempty" db:"result_xdr"`
	MetaXDR           string    `json:"metaXdr,omitempty" db:"meta_xdr"`
	FeeChargedStroops int64     `json:"feeChargedStroops,omitempty" db:"fee_charged_stroops"`
	LedgerNumber      int64     `json:"ledgerNumber,omitempty" db:"ledger_number"`
	LedgerCreatedAt   time.Time `json:"ledgerCreatedAt,omitempty" db:"ledger_created_at"`
	IngestedAt        time.Time `json:"ingestedAt,omitempty" db:"ingested_at"`
	// Relationships:
	Operations   []Operation   `json:"operations,omitempty" db:"operations"`
	Accounts     []Account     `json:"accounts,omitempty" db:"accounts"`
	StateChanges []StateChange `json:"stateChanges,omitempty" db:"state_changes"`
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
	OperationTypeExtendFootprintTtl            OperationType = "EXTEND_FOOTPRINT_TTL"
	OperationTypeRestoreFootprint              OperationType = "RESTORE_FOOTPRINT"
)

type Operation struct {
	ID              string        `json:"id,omitempty" db:"id"`
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
	StateChangeReasonRevoke     StateChangeReason = "REVOKE"
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
	LedgerNumber        int64               `json:"ledgerNumber,omitempty" db:"ledger_number"`
	// Nullable fields:
	Asset              sql.NullString `json:"asset,omitempty" db:"asset"`
	Amount             sql.NullString `json:"amount,omitempty" db:"amount"`
	ClaimableBalanceID sql.NullString `json:"claimableBalanceId,omitempty" db:"claimable_balance_id"`
	ContractID         sql.NullString `json:"contractId,omitempty" db:"contract_id"`
	OfferID            sql.NullString `json:"offerId,omitempty" db:"offer_id"`
	SignerAccountID    sql.NullString `json:"signerAccountId,omitempty" db:"signer_account_id"`
	SignerWeight       sql.NullInt64  `json:"signerWeight,omitempty" db:"signer_weight"`
	SpenderAccountID   sql.NullString `json:"spenderAccountId,omitempty" db:"spender_account_id"`
	TargetAccountID    sql.NullString `json:"targetAccountId,omitempty" db:"target_account_id"`
	Thresholds         sql.NullString `json:"thresholds,omitempty" db:"thresholds"`
	// Nullable JSONB fields: // TODO: update from `NullableJSONB` to custom objects, except for KeyValue.
	ContractInvocation     NullableJSONB `json:"contractInvocation,omitempty" db:"contract_invocation"`
	ContractSubInvocations NullableJSONB `json:"contractSubInvocations,omitempty" db:"contract_sub_invocations"`
	Flags                  NullableJSONB `json:"flags,omitempty" db:"flags"`
	KeyValue               NullableJSONB `json:"keyValue,omitempty" db:"key_value"`
	// Relationships:
	AccountID   string       `json:"accountId,omitempty" db:"account_id"`
	Account     *Account     `json:"account,omitempty" db:"account"`
	OperationID string       `json:"operationId,omitempty" db:"operation_id"`
	Operation   *Operation   `json:"operation,omitempty" db:"operation"`
	TxHash      string       `json:"txHash,omitempty" db:"tx_hash"`
	Transaction *Transaction `json:"transaction,omitempty" db:"transaction"`
}

type NullableJSONB map[string]any

var _ sql.Scanner = (*NullableJSONB)(nil)

func (n *NullableJSONB) Scan(value interface{}) error {
	if value == nil {
		*n = nil
		return nil
	}

	return json.Unmarshal(value.([]byte), n)
}

func (n NullableJSONB) Value() (driver.Value, error) {
	return json.Marshal(n)
}

var _ driver.Valuer = (*NullableJSONB)(nil)
