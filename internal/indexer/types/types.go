package types

import (
	"database/sql"
	"time"
)

type Transaction struct {
	Hash              string    `json:"hash"`
	EnvelopeXDR       string    `json:"envelope_xdr"`
	ResultXDR         string    `json:"result_xdr"`
	MetaXDR           string    `json:"meta_xdr"`
	FeeChargedStroops int64     `json:"fee_charged_stroops"`
	LedgerNumber      int64     `json:"ledger_number"`
	LedgerCreatedAt   time.Time `json:"ledger_created_at"`
	IngestedAt        time.Time `json:"ingested_at"`
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
	ID              string        `json:"id"`
	TxHash          string        `json:"tx_hash"`
	OperationType   OperationType `json:"operation_type"`
	OperationXDR    string        `json:"operation_xdr"`
	LedgerCreatedAt time.Time     `json:"ledger_created_at"`
	IngestedAt      time.Time     `json:"ingested_at"`
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
	ID                  string              `json:"id"`
	StateChangeCategory StateChangeCategory `json:"state_change_category"`
	StateChangeReason   *StateChangeReason  `json:"state_change_reason"`
	IngestedAt          time.Time           `json:"ingested_at"`
	LedgerCreatedAt     time.Time           `json:"ledger_created_at"`
	LedgerNumber        int64               `json:"ledger_number"`
	AccountID           string              `json:"account_id"`
	OperationID         string              `json:"operation_id"`
	TxHash              string              `json:"tx_hash"`
	// Nullable fields:
	Asset              sql.NullString `json:"asset"`
	Amount             sql.NullString `json:"amount"`
	ClaimableBalanceID sql.NullString `json:"claimable_balance_id"`
	ContractID         sql.NullString `json:"contract_id"`
	OfferID            sql.NullString `json:"offer_id"`
	SignerAccountID    sql.NullString `json:"signer_account_id"`
	SignerWeight       sql.NullInt64  `json:"signer_weight"`
	SpenderAccountID   sql.NullString `json:"spender_account_id"`
	TargetAccountID    sql.NullString `json:"target_account_id"`
	Thresholds         sql.NullString `json:"thresholds"`
	// Nullable JSONB fields:
	ContractInvocation     sql.NullString `json:"contract_invocation"`
	ContractSubInvocations sql.NullString `json:"contract_sub_invocations"`
	Flags                  sql.NullString `json:"flags"`
	KeyValue               sql.NullString `json:"keyvalue"`
}
