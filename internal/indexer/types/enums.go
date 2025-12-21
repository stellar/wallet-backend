// Package types provides enum mappings for database storage as SMALLINT.
// These mappings convert between string enum values used in Go code and
// integer values stored in PostgreSQL for storage efficiency.
package types

// StateChangeCategory SMALLINT mappings
var stateChangeCategoryToInt = map[StateChangeCategory]int16{
	StateChangeCategoryBalance:              1,
	StateChangeCategoryAccount:              2,
	StateChangeCategorySigner:               3,
	StateChangeCategorySignatureThreshold:   4,
	StateChangeCategoryMetadata:             5,
	StateChangeCategoryFlags:                6,
	StateChangeCategoryTrustline:            7,
	StateChangeCategoryReserves:             8,
	StateChangeCategoryBalanceAuthorization: 9,
	StateChangeCategoryAuthorization:        10,
}

var intToStateChangeCategory = map[int16]StateChangeCategory{
	1:  StateChangeCategoryBalance,
	2:  StateChangeCategoryAccount,
	3:  StateChangeCategorySigner,
	4:  StateChangeCategorySignatureThreshold,
	5:  StateChangeCategoryMetadata,
	6:  StateChangeCategoryFlags,
	7:  StateChangeCategoryTrustline,
	8:  StateChangeCategoryReserves,
	9:  StateChangeCategoryBalanceAuthorization,
	10: StateChangeCategoryAuthorization,
}

// ToInt16 converts StateChangeCategory to its SMALLINT database representation.
func (c StateChangeCategory) ToInt16() int16 {
	return stateChangeCategoryToInt[c]
}

// StateChangeCategoryFromInt16 converts a SMALLINT database value to StateChangeCategory.
func StateChangeCategoryFromInt16(v int16) StateChangeCategory {
	return intToStateChangeCategory[v]
}

// StateChangeReason SMALLINT mappings
var stateChangeReasonToInt = map[StateChangeReason]int16{
	StateChangeReasonCreate:     1,
	StateChangeReasonMerge:      2,
	StateChangeReasonDebit:      3,
	StateChangeReasonCredit:     4,
	StateChangeReasonMint:       5,
	StateChangeReasonBurn:       6,
	StateChangeReasonAdd:        7,
	StateChangeReasonRemove:     8,
	StateChangeReasonUpdate:     9,
	StateChangeReasonLow:        10,
	StateChangeReasonMedium:     11,
	StateChangeReasonHigh:       12,
	StateChangeReasonHomeDomain: 13,
	StateChangeReasonSet:        14,
	StateChangeReasonClear:      15,
	StateChangeReasonDataEntry:  16,
	StateChangeReasonSponsor:    17,
	StateChangeReasonUnsponsor:  18,
}

var intToStateChangeReason = map[int16]StateChangeReason{
	1:  StateChangeReasonCreate,
	2:  StateChangeReasonMerge,
	3:  StateChangeReasonDebit,
	4:  StateChangeReasonCredit,
	5:  StateChangeReasonMint,
	6:  StateChangeReasonBurn,
	7:  StateChangeReasonAdd,
	8:  StateChangeReasonRemove,
	9:  StateChangeReasonUpdate,
	10: StateChangeReasonLow,
	11: StateChangeReasonMedium,
	12: StateChangeReasonHigh,
	13: StateChangeReasonHomeDomain,
	14: StateChangeReasonSet,
	15: StateChangeReasonClear,
	16: StateChangeReasonDataEntry,
	17: StateChangeReasonSponsor,
	18: StateChangeReasonUnsponsor,
}

// ToInt16 converts StateChangeReason to its SMALLINT database representation.
func (r StateChangeReason) ToInt16() int16 {
	return stateChangeReasonToInt[r]
}

// StateChangeReasonFromInt16 converts a SMALLINT database value to StateChangeReason.
func StateChangeReasonFromInt16(v int16) StateChangeReason {
	return intToStateChangeReason[v]
}

// OperationType SMALLINT mappings
var operationTypeToInt = map[OperationType]int16{
	OperationTypeCreateAccount:                 1,
	OperationTypePayment:                       2,
	OperationTypePathPaymentStrictReceive:      3,
	OperationTypeManageSellOffer:               4,
	OperationTypeCreatePassiveSellOffer:        5,
	OperationTypeSetOptions:                    6,
	OperationTypeChangeTrust:                   7,
	OperationTypeAllowTrust:                    8,
	OperationTypeAccountMerge:                  9,
	OperationTypeInflation:                     10,
	OperationTypeManageData:                    11,
	OperationTypeBumpSequence:                  12,
	OperationTypeManageBuyOffer:                13,
	OperationTypePathPaymentStrictSend:         14,
	OperationTypeCreateClaimableBalance:        15,
	OperationTypeClaimClaimableBalance:         16,
	OperationTypeBeginSponsoringFutureReserves: 17,
	OperationTypeEndSponsoringFutureReserves:   18,
	OperationTypeRevokeSponsorship:             19,
	OperationTypeClawback:                      20,
	OperationTypeClawbackClaimableBalance:      21,
	OperationTypeSetTrustLineFlags:             22,
	OperationTypeLiquidityPoolDeposit:          23,
	OperationTypeLiquidityPoolWithdraw:         24,
	OperationTypeInvokeHostFunction:            25,
	OperationTypeExtendFootprintTTL:            26,
	OperationTypeRestoreFootprint:              27,
}

var intToOperationType = map[int16]OperationType{
	1:  OperationTypeCreateAccount,
	2:  OperationTypePayment,
	3:  OperationTypePathPaymentStrictReceive,
	4:  OperationTypeManageSellOffer,
	5:  OperationTypeCreatePassiveSellOffer,
	6:  OperationTypeSetOptions,
	7:  OperationTypeChangeTrust,
	8:  OperationTypeAllowTrust,
	9:  OperationTypeAccountMerge,
	10: OperationTypeInflation,
	11: OperationTypeManageData,
	12: OperationTypeBumpSequence,
	13: OperationTypeManageBuyOffer,
	14: OperationTypePathPaymentStrictSend,
	15: OperationTypeCreateClaimableBalance,
	16: OperationTypeClaimClaimableBalance,
	17: OperationTypeBeginSponsoringFutureReserves,
	18: OperationTypeEndSponsoringFutureReserves,
	19: OperationTypeRevokeSponsorship,
	20: OperationTypeClawback,
	21: OperationTypeClawbackClaimableBalance,
	22: OperationTypeSetTrustLineFlags,
	23: OperationTypeLiquidityPoolDeposit,
	24: OperationTypeLiquidityPoolWithdraw,
	25: OperationTypeInvokeHostFunction,
	26: OperationTypeExtendFootprintTTL,
	27: OperationTypeRestoreFootprint,
}

// ToInt16 converts OperationType to its SMALLINT database representation.
func (o OperationType) ToInt16() int16 {
	return operationTypeToInt[o]
}

// OperationTypeFromInt16 converts a SMALLINT database value to OperationType.
func OperationTypeFromInt16(v int16) OperationType {
	return intToOperationType[v]
}
