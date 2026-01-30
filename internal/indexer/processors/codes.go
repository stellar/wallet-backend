// Package processors provides helper functions for extracting operation result codes.
// This file contains functions to convert XDR operation results to human-readable string codes.
package processors

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// Common operation result code strings
const (
	OpSuccess       = "op_success"
	OpMalformed     = "op_malformed"
	OpUnderfunded   = "op_underfunded"
	OpLowReserve    = "op_low_reserve"
	OpLineFull      = "op_line_full"
	OpNoIssuer      = "op_no_issuer"
	OpNoTrust       = "op_no_trust"
	OpNotAuthorized = "op_not_authorized"
	OpDoesNotExist  = "op_does_not_exist"
)

// ForOperationResult returns the string representation of an operation result code.
// It returns (resultCode, successful, error).
func ForOperationResult(opr xdr.OperationResult) (string, bool, error) {
	// Handle outer result codes (op_bad_auth, op_no_source_account, etc.)
	if opr.Code != xdr.OperationResultCodeOpInner {
		code, err := stringForOperationResultCode(opr.Code)
		return code, false, err
	}

	// Handle inner result codes based on operation type
	ir := opr.MustTr()
	code, err := stringForInnerResult(ir)
	if err != nil {
		return "", false, err
	}

	successful := code == OpSuccess
	return code, successful, nil
}

// stringForOperationResultCode converts outer operation result codes to strings.
func stringForOperationResultCode(code xdr.OperationResultCode) (string, error) {
	switch code {
	case xdr.OperationResultCodeOpInner:
		return "op_inner", nil
	case xdr.OperationResultCodeOpBadAuth:
		return "op_bad_auth", nil
	case xdr.OperationResultCodeOpNoAccount:
		return "op_no_source_account", nil
	case xdr.OperationResultCodeOpNotSupported:
		return "op_not_supported", nil
	case xdr.OperationResultCodeOpTooManySubentries:
		return "op_too_many_subentries", nil
	case xdr.OperationResultCodeOpExceededWorkLimit:
		return "op_exceeded_work_limit", nil
	default:
		return "", fmt.Errorf("unknown operation result code: %d", code)
	}
}

// stringForInnerResult converts inner operation result codes to strings.
func stringForInnerResult(ir xdr.OperationResultTr) (string, error) {
	switch ir.Type {
	case xdr.OperationTypeCreateAccount:
		return stringForCreateAccountResult(ir.MustCreateAccountResult().Code)
	case xdr.OperationTypePayment:
		return stringForPaymentResult(ir.MustPaymentResult().Code)
	case xdr.OperationTypePathPaymentStrictReceive:
		return stringForPathPaymentStrictReceiveResult(ir.MustPathPaymentStrictReceiveResult().Code)
	case xdr.OperationTypeManageBuyOffer:
		return stringForManageBuyOfferResult(ir.MustManageBuyOfferResult().Code)
	case xdr.OperationTypeManageSellOffer:
		return stringForManageSellOfferResult(ir.MustManageSellOfferResult().Code)
	case xdr.OperationTypeCreatePassiveSellOffer:
		return stringForManageSellOfferResult(ir.MustCreatePassiveSellOfferResult().Code)
	case xdr.OperationTypeSetOptions:
		return stringForSetOptionsResult(ir.MustSetOptionsResult().Code)
	case xdr.OperationTypeChangeTrust:
		return stringForChangeTrustResult(ir.MustChangeTrustResult().Code)
	case xdr.OperationTypeAllowTrust:
		return stringForAllowTrustResult(ir.MustAllowTrustResult().Code)
	case xdr.OperationTypeAccountMerge:
		return stringForAccountMergeResult(ir.MustAccountMergeResult().Code)
	case xdr.OperationTypeInflation:
		return stringForInflationResult(ir.MustInflationResult().Code)
	case xdr.OperationTypeManageData:
		return stringForManageDataResult(ir.MustManageDataResult().Code)
	case xdr.OperationTypeBumpSequence:
		return stringForBumpSequenceResult(ir.MustBumpSeqResult().Code)
	case xdr.OperationTypePathPaymentStrictSend:
		return stringForPathPaymentStrictSendResult(ir.MustPathPaymentStrictSendResult().Code)
	case xdr.OperationTypeCreateClaimableBalance:
		return stringForCreateClaimableBalanceResult(ir.MustCreateClaimableBalanceResult().Code)
	case xdr.OperationTypeClaimClaimableBalance:
		return stringForClaimClaimableBalanceResult(ir.MustClaimClaimableBalanceResult().Code)
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		return stringForBeginSponsoringResult(ir.MustBeginSponsoringFutureReservesResult().Code)
	case xdr.OperationTypeEndSponsoringFutureReserves:
		return stringForEndSponsoringResult(ir.MustEndSponsoringFutureReservesResult().Code)
	case xdr.OperationTypeRevokeSponsorship:
		return stringForRevokeSponsorshipResult(ir.MustRevokeSponsorshipResult().Code)
	case xdr.OperationTypeClawback:
		return stringForClawbackResult(ir.MustClawbackResult().Code)
	case xdr.OperationTypeClawbackClaimableBalance:
		return stringForClawbackClaimableBalanceResult(ir.MustClawbackClaimableBalanceResult().Code)
	case xdr.OperationTypeSetTrustLineFlags:
		return stringForSetTrustLineFlagsResult(ir.MustSetTrustLineFlagsResult().Code)
	case xdr.OperationTypeLiquidityPoolDeposit:
		return stringForLiquidityPoolDepositResult(ir.MustLiquidityPoolDepositResult().Code)
	case xdr.OperationTypeLiquidityPoolWithdraw:
		return stringForLiquidityPoolWithdrawResult(ir.MustLiquidityPoolWithdrawResult().Code)
	case xdr.OperationTypeInvokeHostFunction:
		return stringForInvokeHostFunctionResult(ir.MustInvokeHostFunctionResult().Code)
	case xdr.OperationTypeExtendFootprintTtl:
		return stringForExtendFootprintTtlResult(ir.MustExtendFootprintTtlResult().Code)
	case xdr.OperationTypeRestoreFootprint:
		return stringForRestoreFootprintResult(ir.MustRestoreFootprintResult().Code)
	default:
		return "", fmt.Errorf("unknown operation type: %d", ir.Type)
	}
}

func stringForCreateAccountResult(code xdr.CreateAccountResultCode) (string, error) {
	switch code {
	case xdr.CreateAccountResultCodeCreateAccountSuccess:
		return OpSuccess, nil
	case xdr.CreateAccountResultCodeCreateAccountMalformed:
		return OpMalformed, nil
	case xdr.CreateAccountResultCodeCreateAccountUnderfunded:
		return OpUnderfunded, nil
	case xdr.CreateAccountResultCodeCreateAccountLowReserve:
		return OpLowReserve, nil
	case xdr.CreateAccountResultCodeCreateAccountAlreadyExist:
		return "op_already_exists", nil
	default:
		return "", fmt.Errorf("unknown create account result code: %d", code)
	}
}

func stringForPaymentResult(code xdr.PaymentResultCode) (string, error) {
	switch code {
	case xdr.PaymentResultCodePaymentSuccess:
		return OpSuccess, nil
	case xdr.PaymentResultCodePaymentMalformed:
		return OpMalformed, nil
	case xdr.PaymentResultCodePaymentUnderfunded:
		return OpUnderfunded, nil
	case xdr.PaymentResultCodePaymentSrcNoTrust:
		return "op_src_no_trust", nil
	case xdr.PaymentResultCodePaymentSrcNotAuthorized:
		return "op_src_not_authorized", nil
	case xdr.PaymentResultCodePaymentNoDestination:
		return "op_no_destination", nil
	case xdr.PaymentResultCodePaymentNoTrust:
		return OpNoTrust, nil
	case xdr.PaymentResultCodePaymentNotAuthorized:
		return OpNotAuthorized, nil
	case xdr.PaymentResultCodePaymentLineFull:
		return OpLineFull, nil
	case xdr.PaymentResultCodePaymentNoIssuer:
		return OpNoIssuer, nil
	default:
		return "", fmt.Errorf("unknown payment result code: %d", code)
	}
}

func stringForPathPaymentStrictReceiveResult(code xdr.PathPaymentStrictReceiveResultCode) (string, error) {
	switch code {
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSuccess:
		return OpSuccess, nil
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveMalformed:
		return OpMalformed, nil
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveUnderfunded:
		return OpUnderfunded, nil
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSrcNoTrust:
		return "op_src_no_trust", nil
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSrcNotAuthorized:
		return "op_src_not_authorized", nil
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveNoDestination:
		return "op_no_destination", nil
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveNoTrust:
		return OpNoTrust, nil
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveNotAuthorized:
		return OpNotAuthorized, nil
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveLineFull:
		return OpLineFull, nil
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveNoIssuer:
		return OpNoIssuer, nil
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveTooFewOffers:
		return "op_too_few_offers", nil
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveOfferCrossSelf:
		return "op_cross_self", nil
	case xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveOverSendmax:
		return "op_over_source_max", nil
	default:
		return "", fmt.Errorf("unknown path payment strict receive result code: %d", code)
	}
}

func stringForManageBuyOfferResult(code xdr.ManageBuyOfferResultCode) (string, error) {
	switch code {
	case xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess:
		return OpSuccess, nil
	case xdr.ManageBuyOfferResultCodeManageBuyOfferMalformed:
		return OpMalformed, nil
	case xdr.ManageBuyOfferResultCodeManageBuyOfferSellNoTrust:
		return "op_sell_no_trust", nil
	case xdr.ManageBuyOfferResultCodeManageBuyOfferBuyNoTrust:
		return "op_buy_no_trust", nil
	case xdr.ManageBuyOfferResultCodeManageBuyOfferSellNotAuthorized:
		return "sell_not_authorized", nil
	case xdr.ManageBuyOfferResultCodeManageBuyOfferBuyNotAuthorized:
		return "buy_not_authorized", nil
	case xdr.ManageBuyOfferResultCodeManageBuyOfferLineFull:
		return OpLineFull, nil
	case xdr.ManageBuyOfferResultCodeManageBuyOfferUnderfunded:
		return OpUnderfunded, nil
	case xdr.ManageBuyOfferResultCodeManageBuyOfferCrossSelf:
		return "op_cross_self", nil
	case xdr.ManageBuyOfferResultCodeManageBuyOfferSellNoIssuer:
		return "op_sell_no_issuer", nil
	case xdr.ManageBuyOfferResultCodeManageBuyOfferBuyNoIssuer:
		return "buy_no_issuer", nil
	case xdr.ManageBuyOfferResultCodeManageBuyOfferNotFound:
		return "op_offer_not_found", nil
	case xdr.ManageBuyOfferResultCodeManageBuyOfferLowReserve:
		return OpLowReserve, nil
	default:
		return "", fmt.Errorf("unknown manage buy offer result code: %d", code)
	}
}

func stringForManageSellOfferResult(code xdr.ManageSellOfferResultCode) (string, error) {
	switch code {
	case xdr.ManageSellOfferResultCodeManageSellOfferSuccess:
		return OpSuccess, nil
	case xdr.ManageSellOfferResultCodeManageSellOfferMalformed:
		return OpMalformed, nil
	case xdr.ManageSellOfferResultCodeManageSellOfferSellNoTrust:
		return "op_sell_no_trust", nil
	case xdr.ManageSellOfferResultCodeManageSellOfferBuyNoTrust:
		return "op_buy_no_trust", nil
	case xdr.ManageSellOfferResultCodeManageSellOfferSellNotAuthorized:
		return "sell_not_authorized", nil
	case xdr.ManageSellOfferResultCodeManageSellOfferBuyNotAuthorized:
		return "buy_not_authorized", nil
	case xdr.ManageSellOfferResultCodeManageSellOfferLineFull:
		return OpLineFull, nil
	case xdr.ManageSellOfferResultCodeManageSellOfferUnderfunded:
		return OpUnderfunded, nil
	case xdr.ManageSellOfferResultCodeManageSellOfferCrossSelf:
		return "op_cross_self", nil
	case xdr.ManageSellOfferResultCodeManageSellOfferSellNoIssuer:
		return "op_sell_no_issuer", nil
	case xdr.ManageSellOfferResultCodeManageSellOfferBuyNoIssuer:
		return "buy_no_issuer", nil
	case xdr.ManageSellOfferResultCodeManageSellOfferNotFound:
		return "op_offer_not_found", nil
	case xdr.ManageSellOfferResultCodeManageSellOfferLowReserve:
		return OpLowReserve, nil
	default:
		return "", fmt.Errorf("unknown manage sell offer result code: %d", code)
	}
}

func stringForSetOptionsResult(code xdr.SetOptionsResultCode) (string, error) {
	switch code {
	case xdr.SetOptionsResultCodeSetOptionsSuccess:
		return OpSuccess, nil
	case xdr.SetOptionsResultCodeSetOptionsLowReserve:
		return OpLowReserve, nil
	case xdr.SetOptionsResultCodeSetOptionsTooManySigners:
		return "op_too_many_signers", nil
	case xdr.SetOptionsResultCodeSetOptionsBadFlags:
		return "op_bad_flags", nil
	case xdr.SetOptionsResultCodeSetOptionsInvalidInflation:
		return "op_invalid_inflation", nil
	case xdr.SetOptionsResultCodeSetOptionsCantChange:
		return "op_cant_change", nil
	case xdr.SetOptionsResultCodeSetOptionsUnknownFlag:
		return "op_unknown_flag", nil
	case xdr.SetOptionsResultCodeSetOptionsThresholdOutOfRange:
		return "op_threshold_out_of_range", nil
	case xdr.SetOptionsResultCodeSetOptionsBadSigner:
		return "op_bad_signer", nil
	case xdr.SetOptionsResultCodeSetOptionsInvalidHomeDomain:
		return "op_invalid_home_domain", nil
	case xdr.SetOptionsResultCodeSetOptionsAuthRevocableRequired:
		return "op_auth_revocable_required", nil
	default:
		return "", fmt.Errorf("unknown set options result code: %d", code)
	}
}

func stringForChangeTrustResult(code xdr.ChangeTrustResultCode) (string, error) {
	switch code {
	case xdr.ChangeTrustResultCodeChangeTrustSuccess:
		return OpSuccess, nil
	case xdr.ChangeTrustResultCodeChangeTrustMalformed:
		return OpMalformed, nil
	case xdr.ChangeTrustResultCodeChangeTrustNoIssuer:
		return OpNoIssuer, nil
	case xdr.ChangeTrustResultCodeChangeTrustInvalidLimit:
		return "op_invalid_limit", nil
	case xdr.ChangeTrustResultCodeChangeTrustLowReserve:
		return OpLowReserve, nil
	case xdr.ChangeTrustResultCodeChangeTrustSelfNotAllowed:
		return "op_self_not_allowed", nil
	case xdr.ChangeTrustResultCodeChangeTrustTrustLineMissing:
		return "op_trust_line_missing", nil
	case xdr.ChangeTrustResultCodeChangeTrustCannotDelete:
		return "op_cannot_delete", nil
	case xdr.ChangeTrustResultCodeChangeTrustNotAuthMaintainLiabilities:
		return "op_not_auth_maintain_liabilities", nil
	default:
		return "", fmt.Errorf("unknown change trust result code: %d", code)
	}
}

func stringForAllowTrustResult(code xdr.AllowTrustResultCode) (string, error) {
	switch code {
	case xdr.AllowTrustResultCodeAllowTrustSuccess:
		return OpSuccess, nil
	case xdr.AllowTrustResultCodeAllowTrustMalformed:
		return OpMalformed, nil
	case xdr.AllowTrustResultCodeAllowTrustNoTrustLine:
		return OpNoTrust, nil
	case xdr.AllowTrustResultCodeAllowTrustTrustNotRequired:
		return "op_not_required", nil
	case xdr.AllowTrustResultCodeAllowTrustCantRevoke:
		return "op_cant_revoke", nil
	case xdr.AllowTrustResultCodeAllowTrustSelfNotAllowed:
		return "op_self_not_allowed", nil
	case xdr.AllowTrustResultCodeAllowTrustLowReserve:
		return OpLowReserve, nil
	default:
		return "", fmt.Errorf("unknown allow trust result code: %d", code)
	}
}

func stringForAccountMergeResult(code xdr.AccountMergeResultCode) (string, error) {
	switch code {
	case xdr.AccountMergeResultCodeAccountMergeSuccess:
		return OpSuccess, nil
	case xdr.AccountMergeResultCodeAccountMergeMalformed:
		return OpMalformed, nil
	case xdr.AccountMergeResultCodeAccountMergeNoAccount:
		return "op_no_account", nil
	case xdr.AccountMergeResultCodeAccountMergeImmutableSet:
		return "op_immutable_set", nil
	case xdr.AccountMergeResultCodeAccountMergeHasSubEntries:
		return "op_has_sub_entries", nil
	case xdr.AccountMergeResultCodeAccountMergeSeqnumTooFar:
		return "op_seq_num_too_far", nil
	case xdr.AccountMergeResultCodeAccountMergeDestFull:
		return "op_dest_full", nil
	case xdr.AccountMergeResultCodeAccountMergeIsSponsor:
		return "op_is_sponsor", nil
	default:
		return "", fmt.Errorf("unknown account merge result code: %d", code)
	}
}

func stringForInflationResult(code xdr.InflationResultCode) (string, error) {
	switch code {
	case xdr.InflationResultCodeInflationSuccess:
		return OpSuccess, nil
	case xdr.InflationResultCodeInflationNotTime:
		return "op_not_time", nil
	default:
		return "", fmt.Errorf("unknown inflation result code: %d", code)
	}
}

func stringForManageDataResult(code xdr.ManageDataResultCode) (string, error) {
	switch code {
	case xdr.ManageDataResultCodeManageDataSuccess:
		return OpSuccess, nil
	case xdr.ManageDataResultCodeManageDataNotSupportedYet:
		return "op_not_supported_yet", nil
	case xdr.ManageDataResultCodeManageDataNameNotFound:
		return "op_data_name_not_found", nil
	case xdr.ManageDataResultCodeManageDataLowReserve:
		return OpLowReserve, nil
	case xdr.ManageDataResultCodeManageDataInvalidName:
		return "op_data_invalid_name", nil
	default:
		return "", fmt.Errorf("unknown manage data result code: %d", code)
	}
}

func stringForBumpSequenceResult(code xdr.BumpSequenceResultCode) (string, error) {
	switch code {
	case xdr.BumpSequenceResultCodeBumpSequenceSuccess:
		return OpSuccess, nil
	case xdr.BumpSequenceResultCodeBumpSequenceBadSeq:
		return "op_bad_seq", nil
	default:
		return "", fmt.Errorf("unknown bump sequence result code: %d", code)
	}
}

func stringForPathPaymentStrictSendResult(code xdr.PathPaymentStrictSendResultCode) (string, error) {
	switch code {
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSuccess:
		return OpSuccess, nil
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendMalformed:
		return OpMalformed, nil
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendUnderfunded:
		return OpUnderfunded, nil
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSrcNoTrust:
		return "op_src_no_trust", nil
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSrcNotAuthorized:
		return "op_src_not_authorized", nil
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendNoDestination:
		return "op_no_destination", nil
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendNoTrust:
		return OpNoTrust, nil
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendNotAuthorized:
		return OpNotAuthorized, nil
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendLineFull:
		return OpLineFull, nil
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendNoIssuer:
		return OpNoIssuer, nil
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendTooFewOffers:
		return "op_too_few_offers", nil
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendOfferCrossSelf:
		return "op_cross_self", nil
	case xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendUnderDestmin:
		return "op_under_dest_min", nil
	default:
		return "", fmt.Errorf("unknown path payment strict send result code: %d", code)
	}
}

func stringForCreateClaimableBalanceResult(code xdr.CreateClaimableBalanceResultCode) (string, error) {
	switch code {
	case xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceSuccess:
		return OpSuccess, nil
	case xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceMalformed:
		return OpMalformed, nil
	case xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceLowReserve:
		return OpLowReserve, nil
	case xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceNoTrust:
		return OpNoTrust, nil
	case xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceNotAuthorized:
		return OpNotAuthorized, nil
	case xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceUnderfunded:
		return OpUnderfunded, nil
	default:
		return "", fmt.Errorf("unknown create claimable balance result code: %d", code)
	}
}

func stringForClaimClaimableBalanceResult(code xdr.ClaimClaimableBalanceResultCode) (string, error) {
	switch code {
	case xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess:
		return OpSuccess, nil
	case xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceDoesNotExist:
		return OpDoesNotExist, nil
	case xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceCannotClaim:
		return "op_cannot_claim", nil
	case xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceLineFull:
		return OpLineFull, nil
	case xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceNoTrust:
		return OpNoTrust, nil
	case xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceNotAuthorized:
		return OpNotAuthorized, nil
	default:
		return "", fmt.Errorf("unknown claim claimable balance result code: %d", code)
	}
}

func stringForBeginSponsoringResult(code xdr.BeginSponsoringFutureReservesResultCode) (string, error) {
	switch code {
	case xdr.BeginSponsoringFutureReservesResultCodeBeginSponsoringFutureReservesSuccess:
		return OpSuccess, nil
	case xdr.BeginSponsoringFutureReservesResultCodeBeginSponsoringFutureReservesMalformed:
		return OpMalformed, nil
	case xdr.BeginSponsoringFutureReservesResultCodeBeginSponsoringFutureReservesAlreadySponsored:
		return "op_already_sponsored", nil
	case xdr.BeginSponsoringFutureReservesResultCodeBeginSponsoringFutureReservesRecursive:
		return "op_recursive", nil
	default:
		return "", fmt.Errorf("unknown begin sponsoring result code: %d", code)
	}
}

func stringForEndSponsoringResult(code xdr.EndSponsoringFutureReservesResultCode) (string, error) {
	switch code {
	case xdr.EndSponsoringFutureReservesResultCodeEndSponsoringFutureReservesSuccess:
		return OpSuccess, nil
	case xdr.EndSponsoringFutureReservesResultCodeEndSponsoringFutureReservesNotSponsored:
		return "op_not_sponsored", nil
	default:
		return "", fmt.Errorf("unknown end sponsoring result code: %d", code)
	}
}

func stringForRevokeSponsorshipResult(code xdr.RevokeSponsorshipResultCode) (string, error) {
	switch code {
	case xdr.RevokeSponsorshipResultCodeRevokeSponsorshipSuccess:
		return OpSuccess, nil
	case xdr.RevokeSponsorshipResultCodeRevokeSponsorshipDoesNotExist:
		return OpDoesNotExist, nil
	case xdr.RevokeSponsorshipResultCodeRevokeSponsorshipNotSponsor:
		return "op_not_sponsor", nil
	case xdr.RevokeSponsorshipResultCodeRevokeSponsorshipLowReserve:
		return OpLowReserve, nil
	case xdr.RevokeSponsorshipResultCodeRevokeSponsorshipOnlyTransferable:
		return "op_only_transferable", nil
	case xdr.RevokeSponsorshipResultCodeRevokeSponsorshipMalformed:
		return OpMalformed, nil
	default:
		return "", fmt.Errorf("unknown revoke sponsorship result code: %d", code)
	}
}

func stringForClawbackResult(code xdr.ClawbackResultCode) (string, error) {
	switch code {
	case xdr.ClawbackResultCodeClawbackSuccess:
		return OpSuccess, nil
	case xdr.ClawbackResultCodeClawbackMalformed:
		return OpMalformed, nil
	case xdr.ClawbackResultCodeClawbackNotClawbackEnabled:
		return "op_not_clawback_enabled", nil
	case xdr.ClawbackResultCodeClawbackNoTrust:
		return OpNoTrust, nil
	case xdr.ClawbackResultCodeClawbackUnderfunded:
		return OpUnderfunded, nil
	default:
		return "", fmt.Errorf("unknown clawback result code: %d", code)
	}
}

func stringForClawbackClaimableBalanceResult(code xdr.ClawbackClaimableBalanceResultCode) (string, error) {
	switch code {
	case xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceSuccess:
		return OpSuccess, nil
	case xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceDoesNotExist:
		return OpDoesNotExist, nil
	case xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceNotIssuer:
		return OpNoIssuer, nil
	case xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceNotClawbackEnabled:
		return "op_not_clawback_enabled", nil
	default:
		return "", fmt.Errorf("unknown clawback claimable balance result code: %d", code)
	}
}

func stringForSetTrustLineFlagsResult(code xdr.SetTrustLineFlagsResultCode) (string, error) {
	switch code {
	case xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsSuccess:
		return OpSuccess, nil
	case xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsMalformed:
		return OpMalformed, nil
	case xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsNoTrustLine:
		return OpNoTrust, nil
	case xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsCantRevoke:
		return "op_cant_revoke", nil
	case xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsInvalidState:
		return "op_invalid_state", nil
	case xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsLowReserve:
		return OpLowReserve, nil
	default:
		return "", fmt.Errorf("unknown set trust line flags result code: %d", code)
	}
}

func stringForLiquidityPoolDepositResult(code xdr.LiquidityPoolDepositResultCode) (string, error) {
	switch code {
	case xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositSuccess:
		return OpSuccess, nil
	case xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositMalformed:
		return OpMalformed, nil
	case xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositNoTrust:
		return OpNoTrust, nil
	case xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositNotAuthorized:
		return OpNotAuthorized, nil
	case xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositUnderfunded:
		return OpUnderfunded, nil
	case xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositLineFull:
		return OpLineFull, nil
	case xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositBadPrice:
		return "op_bad_price", nil
	case xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositPoolFull:
		return "op_pool_full", nil
	default:
		return "", fmt.Errorf("unknown liquidity pool deposit result code: %d", code)
	}
}

func stringForLiquidityPoolWithdrawResult(code xdr.LiquidityPoolWithdrawResultCode) (string, error) {
	switch code {
	case xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawSuccess:
		return OpSuccess, nil
	case xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawMalformed:
		return OpMalformed, nil
	case xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawNoTrust:
		return OpNoTrust, nil
	case xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawUnderfunded:
		return OpUnderfunded, nil
	case xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawLineFull:
		return OpLineFull, nil
	case xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawUnderMinimum:
		return "op_under_minimum", nil
	default:
		return "", fmt.Errorf("unknown liquidity pool withdraw result code: %d", code)
	}
}

func stringForInvokeHostFunctionResult(code xdr.InvokeHostFunctionResultCode) (string, error) {
	switch code {
	case xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess:
		return OpSuccess, nil
	case xdr.InvokeHostFunctionResultCodeInvokeHostFunctionMalformed:
		return OpMalformed, nil
	case xdr.InvokeHostFunctionResultCodeInvokeHostFunctionTrapped:
		return "function_trapped", nil
	case xdr.InvokeHostFunctionResultCodeInvokeHostFunctionResourceLimitExceeded:
		return "resource_limit_exceeded", nil
	case xdr.InvokeHostFunctionResultCodeInvokeHostFunctionEntryArchived:
		return "entry_archived", nil
	case xdr.InvokeHostFunctionResultCodeInvokeHostFunctionInsufficientRefundableFee:
		return "insufficient_refundable_fee", nil
	default:
		return "", fmt.Errorf("unknown invoke host function result code: %d", code)
	}
}

func stringForExtendFootprintTtlResult(code xdr.ExtendFootprintTtlResultCode) (string, error) {
	switch code {
	case xdr.ExtendFootprintTtlResultCodeExtendFootprintTtlSuccess:
		return OpSuccess, nil
	case xdr.ExtendFootprintTtlResultCodeExtendFootprintTtlMalformed:
		return OpMalformed, nil
	case xdr.ExtendFootprintTtlResultCodeExtendFootprintTtlResourceLimitExceeded:
		return "resource_limit_exceeded", nil
	case xdr.ExtendFootprintTtlResultCodeExtendFootprintTtlInsufficientRefundableFee:
		return "insufficient_refundable_fee", nil
	default:
		return "", fmt.Errorf("unknown extend footprint ttl result code: %d", code)
	}
}

func stringForRestoreFootprintResult(code xdr.RestoreFootprintResultCode) (string, error) {
	switch code {
	case xdr.RestoreFootprintResultCodeRestoreFootprintSuccess:
		return OpSuccess, nil
	case xdr.RestoreFootprintResultCodeRestoreFootprintMalformed:
		return OpMalformed, nil
	case xdr.RestoreFootprintResultCodeRestoreFootprintResourceLimitExceeded:
		return "resource_limit_exceeded", nil
	case xdr.RestoreFootprintResultCodeRestoreFootprintInsufficientRefundableFee:
		return "insufficient_refundable_fee", nil
	default:
		return "", fmt.Errorf("unknown restore footprint result code: %d", code)
	}
}
