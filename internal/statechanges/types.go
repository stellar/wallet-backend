package statechanges

import (
	"time"
)

type Transaction struct {
	TxHash         string    `db:"txhash"`
	TransactionXDR string    `db:"transactionxdr"`
	TxMeta         *string   `db:"txmeta"`
	TS             time.Time `db:"ts"`
}

type Operation struct {
	ToID           string    `db:"toid"`
	TxHash         string    `db:"txhash"`
	OperationIndex int       `db:"operationindex"`
	OperationType  int       `db:"operationtype"`
	TS             time.Time `db:"ts"`
}

type StateChangeType string

const (
	StateChangeDebit                StateChangeType = "DEBIT"
	StateChangeCredit               StateChangeType = "CREDIT"
	StateChangeHomeDomain           StateChangeType = "HOMEDOMAIN"
	StateChangeFlags                StateChangeType = "FLAGS"
	StateChangeSigners              StateChangeType = "SIGNERS"
	StateChangeDataEntry            StateChangeType = "DATAENTRY"
	StateChangeAllowance            StateChangeType = "ALLOWANCE"
	StateChangeBalanceAuthorization StateChangeType = "BALANCEAUTHORIZATION"
	StateChangeThresholds           StateChangeType = "THRESHOLDS"
	StateChangeAuthorization        StateChangeType = "AUTHORIZATION"
)

type StateChangeReason string

const (
	ReasonCreateAccount          StateChangeReason = "CREATEACCOUNT"
	ReasonMergeAccount           StateChangeReason = "MERGEACCOUNT"
	ReasonPayment                StateChangeReason = "PAYMENT"
	ReasonCreateClaimableBalance StateChangeReason = "CREATECLAIMABLEBALANCE"
	ReasonClaimClaimableBalance  StateChangeReason = "CLAIMCLAIMABLEBALANCE"
	ReasonClawback               StateChangeReason = "CLAWBACK"
	ReasonLiquidityPoolDeposit   StateChangeReason = "LIQUIDITYPOOLDEPOSIT"
	ReasonLiquidityPoolWithdraw  StateChangeReason = "LIQUIDITYPOOLWITHDRAW"
	ReasonOffers                 StateChangeReason = "OFFERS"
	ReasonPathPayment            StateChangeReason = "PATHPAYMENT"
	ReasonFee                    StateChangeReason = "FEE"
	ReasonTransfer               StateChangeReason = "TRANSFER"
	ReasonContractTransfer       StateChangeReason = "CONTRACT_TRANSFER"
	ReasonContractMint           StateChangeReason = "CONTRACT_MINT"
	ReasonContractBurn           StateChangeReason = "CONTRACT_BURN"
	ReasonContractClawback       StateChangeReason = "CONTRACT_CLAWBACK"
	ReasonFlagsCleared           StateChangeReason = "FLAGS_CLEARED"
	ReasonFlagsSet               StateChangeReason = "FLAGS_SET"
	ReasonSignersAdded           StateChangeReason = "SIGNERS_ADDED"
	ReasonSignersUpdated         StateChangeReason = "SIGNERS_UPDATED"
	ReasonSignersRemoved         StateChangeReason = "SIGNERS_REMOVED"
	ReasonAllowanceSet           StateChangeReason = "ALLOWANCE_SET"
	ReasonAllowanceConsumed      StateChangeReason = "ALLOWANCE_CONSUMED"
)

type StateChange struct {
	ID                   int64             `db:"id"`
	AcctID               string            `db:"acctid"`
	OperationTOID        *int64            `db:"operationtoid"`
	TxHash               string            `db:"txhash"`
	StateChangeType      StateChangeType   `db:"statechangetype"`
	StateChangeReason    StateChangeReason `db:"statechangereason"`
	ChangeMetadata       string            `db:"changemetadata"` // JSONB stored as string
	Asset                *string           `db:"asset"`
	Amount               *float64          `db:"amount"`
	LowThreshold         *int              `db:"lowthreshold"`
	MediumThreshold      *int              `db:"mediumthreshold"`
	HighThreshold        *int              `db:"highthreshold"`
	FlagsCleared         []string          `db:"flags_cleared"`
	FlagsSet             []string          `db:"flags_set"`
	HomeDomain           *string           `db:"homedomain"`
	SignerAdded          string            `db:"signer_added"`   // JSONB stored as string
	SignerRemoved        string            `db:"signer_removed"` // JSONB stored as string
	SignerUpdated        string            `db:"signer_updated"` // JSONB stored as string
	DataEntry            string            `db:"data_entry"`     // JSONB stored as string
	BalanceAuthorization *bool             `db:"balance_authorization"`
	ContractAddress      *string           `db:"contract_address"`
	FunctionName         *string           `db:"function_name"`
	TS                   time.Time         `db:"ts"`
}
