// Tests for operation result code extraction
// Verifies ForOperationResult correctly converts XDR operation results to string codes
package processors

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringForOperationResultCode(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.OperationResultCode
		expected string
		wantErr  bool
	}{
		{name: "op_inner", code: xdr.OperationResultCodeOpInner, expected: "op_inner"},
		{name: "op_bad_auth", code: xdr.OperationResultCodeOpBadAuth, expected: "op_bad_auth"},
		{name: "op_no_source_account", code: xdr.OperationResultCodeOpNoAccount, expected: "op_no_source_account"},
		{name: "op_not_supported", code: xdr.OperationResultCodeOpNotSupported, expected: "op_not_supported"},
		{name: "op_too_many_subentries", code: xdr.OperationResultCodeOpTooManySubentries, expected: "op_too_many_subentries"},
		{name: "op_exceeded_work_limit", code: xdr.OperationResultCodeOpExceededWorkLimit, expected: "op_exceeded_work_limit"},
		{name: "op_too_many_sponsoring", code: xdr.OperationResultCodeOpTooManySponsoring, expected: "op_too_many_sponsoring"},
		{name: "unknown code", code: xdr.OperationResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForOperationResultCode(tt.code)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "unknown operation result code")
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestForOperationResult_OuterCodes(t *testing.T) {
	tests := []struct {
		name         string
		code         xdr.OperationResultCode
		expectedCode string
		expectedSucc bool
	}{
		{name: "op_bad_auth", code: xdr.OperationResultCodeOpBadAuth, expectedCode: "op_bad_auth", expectedSucc: false},
		{name: "op_no_source_account", code: xdr.OperationResultCodeOpNoAccount, expectedCode: "op_no_source_account", expectedSucc: false},
		{name: "op_not_supported", code: xdr.OperationResultCodeOpNotSupported, expectedCode: "op_not_supported", expectedSucc: false},
		{name: "op_too_many_subentries", code: xdr.OperationResultCodeOpTooManySubentries, expectedCode: "op_too_many_subentries", expectedSucc: false},
		{name: "op_exceeded_work_limit", code: xdr.OperationResultCodeOpExceededWorkLimit, expectedCode: "op_exceeded_work_limit", expectedSucc: false},
		{name: "op_too_many_sponsoring", code: xdr.OperationResultCodeOpTooManySponsoring, expectedCode: "op_too_many_sponsoring", expectedSucc: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opr := xdr.OperationResult{Code: tt.code}
			code, succ, err := forOperationResult(opr)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedCode, code)
			assert.Equal(t, tt.expectedSucc, succ)
		})
	}
}

func TestStringForCreateAccountResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.CreateAccountResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.CreateAccountResultCodeCreateAccountSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.CreateAccountResultCodeCreateAccountMalformed, expected: OpMalformed},
		{name: "underfunded", code: xdr.CreateAccountResultCodeCreateAccountUnderfunded, expected: OpUnderfunded},
		{name: "low_reserve", code: xdr.CreateAccountResultCodeCreateAccountLowReserve, expected: OpLowReserve},
		{name: "already_exists", code: xdr.CreateAccountResultCodeCreateAccountAlreadyExist, expected: "op_already_exists"},
		{name: "unknown", code: xdr.CreateAccountResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForCreateAccountResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForPaymentResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.PaymentResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.PaymentResultCodePaymentSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.PaymentResultCodePaymentMalformed, expected: OpMalformed},
		{name: "underfunded", code: xdr.PaymentResultCodePaymentUnderfunded, expected: OpUnderfunded},
		{name: "src_no_trust", code: xdr.PaymentResultCodePaymentSrcNoTrust, expected: "op_src_no_trust"},
		{name: "src_not_authorized", code: xdr.PaymentResultCodePaymentSrcNotAuthorized, expected: "op_src_not_authorized"},
		{name: "no_destination", code: xdr.PaymentResultCodePaymentNoDestination, expected: "op_no_destination"},
		{name: "no_trust", code: xdr.PaymentResultCodePaymentNoTrust, expected: OpNoTrust},
		{name: "not_authorized", code: xdr.PaymentResultCodePaymentNotAuthorized, expected: OpNotAuthorized},
		{name: "line_full", code: xdr.PaymentResultCodePaymentLineFull, expected: OpLineFull},
		{name: "no_issuer", code: xdr.PaymentResultCodePaymentNoIssuer, expected: OpNoIssuer},
		{name: "unknown", code: xdr.PaymentResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForPaymentResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForPathPaymentStrictReceiveResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.PathPaymentStrictReceiveResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveMalformed, expected: OpMalformed},
		{name: "underfunded", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveUnderfunded, expected: OpUnderfunded},
		{name: "src_no_trust", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSrcNoTrust, expected: "op_src_no_trust"},
		{name: "src_not_authorized", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSrcNotAuthorized, expected: "op_src_not_authorized"},
		{name: "no_destination", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveNoDestination, expected: "op_no_destination"},
		{name: "no_trust", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveNoTrust, expected: OpNoTrust},
		{name: "not_authorized", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveNotAuthorized, expected: OpNotAuthorized},
		{name: "line_full", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveLineFull, expected: OpLineFull},
		{name: "no_issuer", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveNoIssuer, expected: OpNoIssuer},
		{name: "too_few_offers", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveTooFewOffers, expected: "op_too_few_offers"},
		{name: "cross_self", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveOfferCrossSelf, expected: "op_cross_self"},
		{name: "over_source_max", code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveOverSendmax, expected: "op_over_source_max"},
		{name: "unknown", code: xdr.PathPaymentStrictReceiveResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForPathPaymentStrictReceiveResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForPathPaymentStrictSendResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.PathPaymentStrictSendResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendMalformed, expected: OpMalformed},
		{name: "underfunded", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendUnderfunded, expected: OpUnderfunded},
		{name: "src_no_trust", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSrcNoTrust, expected: "op_src_no_trust"},
		{name: "src_not_authorized", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSrcNotAuthorized, expected: "op_src_not_authorized"},
		{name: "no_destination", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendNoDestination, expected: "op_no_destination"},
		{name: "no_trust", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendNoTrust, expected: OpNoTrust},
		{name: "not_authorized", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendNotAuthorized, expected: OpNotAuthorized},
		{name: "line_full", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendLineFull, expected: OpLineFull},
		{name: "no_issuer", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendNoIssuer, expected: OpNoIssuer},
		{name: "too_few_offers", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendTooFewOffers, expected: "op_too_few_offers"},
		{name: "cross_self", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendOfferCrossSelf, expected: "op_cross_self"},
		{name: "under_dest_min", code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendUnderDestmin, expected: "op_under_dest_min"},
		{name: "unknown", code: xdr.PathPaymentStrictSendResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForPathPaymentStrictSendResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForManageBuyOfferResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.ManageBuyOfferResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.ManageBuyOfferResultCodeManageBuyOfferMalformed, expected: OpMalformed},
		{name: "sell_no_trust", code: xdr.ManageBuyOfferResultCodeManageBuyOfferSellNoTrust, expected: "op_sell_no_trust"},
		{name: "buy_no_trust", code: xdr.ManageBuyOfferResultCodeManageBuyOfferBuyNoTrust, expected: "op_buy_no_trust"},
		{name: "sell_not_authorized", code: xdr.ManageBuyOfferResultCodeManageBuyOfferSellNotAuthorized, expected: "sell_not_authorized"},
		{name: "buy_not_authorized", code: xdr.ManageBuyOfferResultCodeManageBuyOfferBuyNotAuthorized, expected: "buy_not_authorized"},
		{name: "line_full", code: xdr.ManageBuyOfferResultCodeManageBuyOfferLineFull, expected: OpLineFull},
		{name: "underfunded", code: xdr.ManageBuyOfferResultCodeManageBuyOfferUnderfunded, expected: OpUnderfunded},
		{name: "cross_self", code: xdr.ManageBuyOfferResultCodeManageBuyOfferCrossSelf, expected: "op_cross_self"},
		{name: "sell_no_issuer", code: xdr.ManageBuyOfferResultCodeManageBuyOfferSellNoIssuer, expected: "op_sell_no_issuer"},
		{name: "buy_no_issuer", code: xdr.ManageBuyOfferResultCodeManageBuyOfferBuyNoIssuer, expected: "buy_no_issuer"},
		{name: "offer_not_found", code: xdr.ManageBuyOfferResultCodeManageBuyOfferNotFound, expected: "op_offer_not_found"},
		{name: "low_reserve", code: xdr.ManageBuyOfferResultCodeManageBuyOfferLowReserve, expected: OpLowReserve},
		{name: "unknown", code: xdr.ManageBuyOfferResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForManageBuyOfferResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForManageSellOfferResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.ManageSellOfferResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.ManageSellOfferResultCodeManageSellOfferSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.ManageSellOfferResultCodeManageSellOfferMalformed, expected: OpMalformed},
		{name: "sell_no_trust", code: xdr.ManageSellOfferResultCodeManageSellOfferSellNoTrust, expected: "op_sell_no_trust"},
		{name: "buy_no_trust", code: xdr.ManageSellOfferResultCodeManageSellOfferBuyNoTrust, expected: "op_buy_no_trust"},
		{name: "sell_not_authorized", code: xdr.ManageSellOfferResultCodeManageSellOfferSellNotAuthorized, expected: "sell_not_authorized"},
		{name: "buy_not_authorized", code: xdr.ManageSellOfferResultCodeManageSellOfferBuyNotAuthorized, expected: "buy_not_authorized"},
		{name: "line_full", code: xdr.ManageSellOfferResultCodeManageSellOfferLineFull, expected: OpLineFull},
		{name: "underfunded", code: xdr.ManageSellOfferResultCodeManageSellOfferUnderfunded, expected: OpUnderfunded},
		{name: "cross_self", code: xdr.ManageSellOfferResultCodeManageSellOfferCrossSelf, expected: "op_cross_self"},
		{name: "sell_no_issuer", code: xdr.ManageSellOfferResultCodeManageSellOfferSellNoIssuer, expected: "op_sell_no_issuer"},
		{name: "buy_no_issuer", code: xdr.ManageSellOfferResultCodeManageSellOfferBuyNoIssuer, expected: "buy_no_issuer"},
		{name: "offer_not_found", code: xdr.ManageSellOfferResultCodeManageSellOfferNotFound, expected: "op_offer_not_found"},
		{name: "low_reserve", code: xdr.ManageSellOfferResultCodeManageSellOfferLowReserve, expected: OpLowReserve},
		{name: "unknown", code: xdr.ManageSellOfferResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForManageSellOfferResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForSetOptionsResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.SetOptionsResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.SetOptionsResultCodeSetOptionsSuccess, expected: OpSuccess},
		{name: "low_reserve", code: xdr.SetOptionsResultCodeSetOptionsLowReserve, expected: OpLowReserve},
		{name: "too_many_signers", code: xdr.SetOptionsResultCodeSetOptionsTooManySigners, expected: "op_too_many_signers"},
		{name: "bad_flags", code: xdr.SetOptionsResultCodeSetOptionsBadFlags, expected: "op_bad_flags"},
		{name: "invalid_inflation", code: xdr.SetOptionsResultCodeSetOptionsInvalidInflation, expected: "op_invalid_inflation"},
		{name: "cant_change", code: xdr.SetOptionsResultCodeSetOptionsCantChange, expected: "op_cant_change"},
		{name: "unknown_flag", code: xdr.SetOptionsResultCodeSetOptionsUnknownFlag, expected: "op_unknown_flag"},
		{name: "threshold_out_of_range", code: xdr.SetOptionsResultCodeSetOptionsThresholdOutOfRange, expected: "op_threshold_out_of_range"},
		{name: "bad_signer", code: xdr.SetOptionsResultCodeSetOptionsBadSigner, expected: "op_bad_signer"},
		{name: "invalid_home_domain", code: xdr.SetOptionsResultCodeSetOptionsInvalidHomeDomain, expected: "op_invalid_home_domain"},
		{name: "auth_revocable_required", code: xdr.SetOptionsResultCodeSetOptionsAuthRevocableRequired, expected: "op_auth_revocable_required"},
		{name: "unknown", code: xdr.SetOptionsResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForSetOptionsResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForChangeTrustResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.ChangeTrustResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.ChangeTrustResultCodeChangeTrustSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.ChangeTrustResultCodeChangeTrustMalformed, expected: OpMalformed},
		{name: "no_issuer", code: xdr.ChangeTrustResultCodeChangeTrustNoIssuer, expected: OpNoIssuer},
		{name: "invalid_limit", code: xdr.ChangeTrustResultCodeChangeTrustInvalidLimit, expected: "op_invalid_limit"},
		{name: "low_reserve", code: xdr.ChangeTrustResultCodeChangeTrustLowReserve, expected: OpLowReserve},
		{name: "self_not_allowed", code: xdr.ChangeTrustResultCodeChangeTrustSelfNotAllowed, expected: "op_self_not_allowed"},
		{name: "trust_line_missing", code: xdr.ChangeTrustResultCodeChangeTrustTrustLineMissing, expected: "op_trust_line_missing"},
		{name: "cannot_delete", code: xdr.ChangeTrustResultCodeChangeTrustCannotDelete, expected: "op_cannot_delete"},
		{name: "not_auth_maintain_liabilities", code: xdr.ChangeTrustResultCodeChangeTrustNotAuthMaintainLiabilities, expected: "op_not_auth_maintain_liabilities"},
		{name: "unknown", code: xdr.ChangeTrustResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForChangeTrustResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForAllowTrustResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.AllowTrustResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.AllowTrustResultCodeAllowTrustSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.AllowTrustResultCodeAllowTrustMalformed, expected: OpMalformed},
		{name: "no_trust_line", code: xdr.AllowTrustResultCodeAllowTrustNoTrustLine, expected: OpNoTrust},
		{name: "not_required", code: xdr.AllowTrustResultCodeAllowTrustTrustNotRequired, expected: "op_not_required"},
		{name: "cant_revoke", code: xdr.AllowTrustResultCodeAllowTrustCantRevoke, expected: "op_cant_revoke"},
		{name: "self_not_allowed", code: xdr.AllowTrustResultCodeAllowTrustSelfNotAllowed, expected: "op_self_not_allowed"},
		{name: "low_reserve", code: xdr.AllowTrustResultCodeAllowTrustLowReserve, expected: OpLowReserve},
		{name: "unknown", code: xdr.AllowTrustResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForAllowTrustResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForAccountMergeResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.AccountMergeResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.AccountMergeResultCodeAccountMergeSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.AccountMergeResultCodeAccountMergeMalformed, expected: OpMalformed},
		{name: "no_account", code: xdr.AccountMergeResultCodeAccountMergeNoAccount, expected: "op_no_account"},
		{name: "immutable_set", code: xdr.AccountMergeResultCodeAccountMergeImmutableSet, expected: "op_immutable_set"},
		{name: "has_sub_entries", code: xdr.AccountMergeResultCodeAccountMergeHasSubEntries, expected: "op_has_sub_entries"},
		{name: "seq_num_too_far", code: xdr.AccountMergeResultCodeAccountMergeSeqnumTooFar, expected: "op_seq_num_too_far"},
		{name: "dest_full", code: xdr.AccountMergeResultCodeAccountMergeDestFull, expected: "op_dest_full"},
		{name: "is_sponsor", code: xdr.AccountMergeResultCodeAccountMergeIsSponsor, expected: "op_is_sponsor"},
		{name: "unknown", code: xdr.AccountMergeResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForAccountMergeResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForInflationResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.InflationResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.InflationResultCodeInflationSuccess, expected: OpSuccess},
		{name: "not_time", code: xdr.InflationResultCodeInflationNotTime, expected: "op_not_time"},
		{name: "unknown", code: xdr.InflationResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForInflationResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForManageDataResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.ManageDataResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.ManageDataResultCodeManageDataSuccess, expected: OpSuccess},
		{name: "not_supported_yet", code: xdr.ManageDataResultCodeManageDataNotSupportedYet, expected: "op_not_supported_yet"},
		{name: "name_not_found", code: xdr.ManageDataResultCodeManageDataNameNotFound, expected: "op_data_name_not_found"},
		{name: "low_reserve", code: xdr.ManageDataResultCodeManageDataLowReserve, expected: OpLowReserve},
		{name: "invalid_name", code: xdr.ManageDataResultCodeManageDataInvalidName, expected: "op_data_invalid_name"},
		{name: "unknown", code: xdr.ManageDataResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForManageDataResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForBumpSequenceResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.BumpSequenceResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.BumpSequenceResultCodeBumpSequenceSuccess, expected: OpSuccess},
		{name: "bad_seq", code: xdr.BumpSequenceResultCodeBumpSequenceBadSeq, expected: "op_bad_seq"},
		{name: "unknown", code: xdr.BumpSequenceResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForBumpSequenceResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForCreateClaimableBalanceResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.CreateClaimableBalanceResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceMalformed, expected: OpMalformed},
		{name: "low_reserve", code: xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceLowReserve, expected: OpLowReserve},
		{name: "no_trust", code: xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceNoTrust, expected: OpNoTrust},
		{name: "not_authorized", code: xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceNotAuthorized, expected: OpNotAuthorized},
		{name: "underfunded", code: xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceUnderfunded, expected: OpUnderfunded},
		{name: "unknown", code: xdr.CreateClaimableBalanceResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForCreateClaimableBalanceResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForClaimClaimableBalanceResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.ClaimClaimableBalanceResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess, expected: OpSuccess},
		{name: "does_not_exist", code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceDoesNotExist, expected: OpDoesNotExist},
		{name: "cannot_claim", code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceCannotClaim, expected: "op_cannot_claim"},
		{name: "line_full", code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceLineFull, expected: OpLineFull},
		{name: "no_trust", code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceNoTrust, expected: OpNoTrust},
		{name: "not_authorized", code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceNotAuthorized, expected: OpNotAuthorized},
		{name: "unknown", code: xdr.ClaimClaimableBalanceResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForClaimClaimableBalanceResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForBeginSponsoringResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.BeginSponsoringFutureReservesResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.BeginSponsoringFutureReservesResultCodeBeginSponsoringFutureReservesSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.BeginSponsoringFutureReservesResultCodeBeginSponsoringFutureReservesMalformed, expected: OpMalformed},
		{name: "already_sponsored", code: xdr.BeginSponsoringFutureReservesResultCodeBeginSponsoringFutureReservesAlreadySponsored, expected: "op_already_sponsored"},
		{name: "recursive", code: xdr.BeginSponsoringFutureReservesResultCodeBeginSponsoringFutureReservesRecursive, expected: "op_recursive"},
		{name: "unknown", code: xdr.BeginSponsoringFutureReservesResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForBeginSponsoringResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForEndSponsoringResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.EndSponsoringFutureReservesResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.EndSponsoringFutureReservesResultCodeEndSponsoringFutureReservesSuccess, expected: OpSuccess},
		{name: "not_sponsored", code: xdr.EndSponsoringFutureReservesResultCodeEndSponsoringFutureReservesNotSponsored, expected: "op_not_sponsored"},
		{name: "unknown", code: xdr.EndSponsoringFutureReservesResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForEndSponsoringResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForRevokeSponsorshipResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.RevokeSponsorshipResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipSuccess, expected: OpSuccess},
		{name: "does_not_exist", code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipDoesNotExist, expected: OpDoesNotExist},
		{name: "not_sponsor", code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipNotSponsor, expected: "op_not_sponsor"},
		{name: "low_reserve", code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipLowReserve, expected: OpLowReserve},
		{name: "only_transferable", code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipOnlyTransferable, expected: "op_only_transferable"},
		{name: "malformed", code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipMalformed, expected: OpMalformed},
		{name: "unknown", code: xdr.RevokeSponsorshipResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForRevokeSponsorshipResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForClawbackResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.ClawbackResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.ClawbackResultCodeClawbackSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.ClawbackResultCodeClawbackMalformed, expected: OpMalformed},
		{name: "not_clawback_enabled", code: xdr.ClawbackResultCodeClawbackNotClawbackEnabled, expected: "op_not_clawback_enabled"},
		{name: "no_trust", code: xdr.ClawbackResultCodeClawbackNoTrust, expected: OpNoTrust},
		{name: "underfunded", code: xdr.ClawbackResultCodeClawbackUnderfunded, expected: OpUnderfunded},
		{name: "unknown", code: xdr.ClawbackResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForClawbackResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForClawbackClaimableBalanceResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.ClawbackClaimableBalanceResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceSuccess, expected: OpSuccess},
		{name: "does_not_exist", code: xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceDoesNotExist, expected: OpDoesNotExist},
		{name: "not_issuer", code: xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceNotIssuer, expected: OpNoIssuer},
		{name: "not_clawback_enabled", code: xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceNotClawbackEnabled, expected: "op_not_clawback_enabled"},
		{name: "unknown", code: xdr.ClawbackClaimableBalanceResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForClawbackClaimableBalanceResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForSetTrustLineFlagsResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.SetTrustLineFlagsResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsMalformed, expected: OpMalformed},
		{name: "no_trust_line", code: xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsNoTrustLine, expected: OpNoTrust},
		{name: "cant_revoke", code: xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsCantRevoke, expected: "op_cant_revoke"},
		{name: "invalid_state", code: xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsInvalidState, expected: "op_invalid_state"},
		{name: "low_reserve", code: xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsLowReserve, expected: OpLowReserve},
		{name: "unknown", code: xdr.SetTrustLineFlagsResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForSetTrustLineFlagsResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForLiquidityPoolDepositResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.LiquidityPoolDepositResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositMalformed, expected: OpMalformed},
		{name: "no_trust", code: xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositNoTrust, expected: OpNoTrust},
		{name: "not_authorized", code: xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositNotAuthorized, expected: OpNotAuthorized},
		{name: "underfunded", code: xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositUnderfunded, expected: OpUnderfunded},
		{name: "line_full", code: xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositLineFull, expected: OpLineFull},
		{name: "bad_price", code: xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositBadPrice, expected: "op_bad_price"},
		{name: "pool_full", code: xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositPoolFull, expected: "op_pool_full"},
		{name: "unknown", code: xdr.LiquidityPoolDepositResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForLiquidityPoolDepositResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForLiquidityPoolWithdrawResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.LiquidityPoolWithdrawResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawMalformed, expected: OpMalformed},
		{name: "no_trust", code: xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawNoTrust, expected: OpNoTrust},
		{name: "underfunded", code: xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawUnderfunded, expected: OpUnderfunded},
		{name: "line_full", code: xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawLineFull, expected: OpLineFull},
		{name: "under_minimum", code: xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawUnderMinimum, expected: "op_under_minimum"},
		{name: "unknown", code: xdr.LiquidityPoolWithdrawResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForLiquidityPoolWithdrawResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForInvokeHostFunctionResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.InvokeHostFunctionResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.InvokeHostFunctionResultCodeInvokeHostFunctionMalformed, expected: OpMalformed},
		{name: "trapped", code: xdr.InvokeHostFunctionResultCodeInvokeHostFunctionTrapped, expected: "function_trapped"},
		{name: "resource_limit_exceeded", code: xdr.InvokeHostFunctionResultCodeInvokeHostFunctionResourceLimitExceeded, expected: "resource_limit_exceeded"},
		{name: "entry_archived", code: xdr.InvokeHostFunctionResultCodeInvokeHostFunctionEntryArchived, expected: "entry_archived"},
		{name: "insufficient_refundable_fee", code: xdr.InvokeHostFunctionResultCodeInvokeHostFunctionInsufficientRefundableFee, expected: "insufficient_refundable_fee"},
		{name: "unknown", code: xdr.InvokeHostFunctionResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForInvokeHostFunctionResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForExtendFootprintTTLResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.ExtendFootprintTtlResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.ExtendFootprintTtlResultCodeExtendFootprintTtlSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.ExtendFootprintTtlResultCodeExtendFootprintTtlMalformed, expected: OpMalformed},
		{name: "resource_limit_exceeded", code: xdr.ExtendFootprintTtlResultCodeExtendFootprintTtlResourceLimitExceeded, expected: "resource_limit_exceeded"},
		{name: "insufficient_refundable_fee", code: xdr.ExtendFootprintTtlResultCodeExtendFootprintTtlInsufficientRefundableFee, expected: "insufficient_refundable_fee"},
		{name: "unknown", code: xdr.ExtendFootprintTtlResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForExtendFootprintTTLResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForRestoreFootprintResult(t *testing.T) {
	tests := []struct {
		name     string
		code     xdr.RestoreFootprintResultCode
		expected string
		wantErr  bool
	}{
		{name: "success", code: xdr.RestoreFootprintResultCodeRestoreFootprintSuccess, expected: OpSuccess},
		{name: "malformed", code: xdr.RestoreFootprintResultCodeRestoreFootprintMalformed, expected: OpMalformed},
		{name: "resource_limit_exceeded", code: xdr.RestoreFootprintResultCodeRestoreFootprintResourceLimitExceeded, expected: "resource_limit_exceeded"},
		{name: "insufficient_refundable_fee", code: xdr.RestoreFootprintResultCodeRestoreFootprintInsufficientRefundableFee, expected: "insufficient_refundable_fee"},
		{name: "unknown", code: xdr.RestoreFootprintResultCode(999), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringForRestoreFootprintResult(tt.code)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestStringForInnerResult_UnknownType(t *testing.T) {
	// Test that unknown operation types return an error
	ir := xdr.OperationResultTr{
		Type: xdr.OperationType(999),
	}
	_, err := stringForInnerResult(ir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown operation type")
}

func TestForOperationResult_InnerError(t *testing.T) {
	// Test that forOperationResult propagates errors from stringForInnerResult
	tr := xdr.OperationResultTr{Type: xdr.OperationType(999)}
	opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
	code, succ, err := forOperationResult(opr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown operation type")
	assert.Equal(t, "", code)
	assert.False(t, succ)
}

func TestForOperationResult_InnerResults(t *testing.T) {
	// Test successful inner results
	t.Run("payment_success", func(t *testing.T) {
		paymentResult := xdr.PaymentResult{Code: xdr.PaymentResultCodePaymentSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypePayment, PaymentResult: &paymentResult}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("payment_failed", func(t *testing.T) {
		paymentResult := xdr.PaymentResult{Code: xdr.PaymentResultCodePaymentUnderfunded}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypePayment, PaymentResult: &paymentResult}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpUnderfunded, code)
		assert.False(t, succ)
	})

	t.Run("create_account_success", func(t *testing.T) {
		result := xdr.CreateAccountResult{Code: xdr.CreateAccountResultCodeCreateAccountSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeCreateAccount, CreateAccountResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("path_payment_strict_receive_success", func(t *testing.T) {
		result := xdr.PathPaymentStrictReceiveResult{Code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypePathPaymentStrictReceive, PathPaymentStrictReceiveResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("manage_buy_offer_success", func(t *testing.T) {
		result := xdr.ManageBuyOfferResult{Code: xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeManageBuyOffer, ManageBuyOfferResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("manage_sell_offer_success", func(t *testing.T) {
		result := xdr.ManageSellOfferResult{Code: xdr.ManageSellOfferResultCodeManageSellOfferSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeManageSellOffer, ManageSellOfferResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("create_passive_sell_offer_success", func(t *testing.T) {
		result := xdr.ManageSellOfferResult{Code: xdr.ManageSellOfferResultCodeManageSellOfferSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeCreatePassiveSellOffer, CreatePassiveSellOfferResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("set_options_success", func(t *testing.T) {
		result := xdr.SetOptionsResult{Code: xdr.SetOptionsResultCodeSetOptionsSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeSetOptions, SetOptionsResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("change_trust_success", func(t *testing.T) {
		result := xdr.ChangeTrustResult{Code: xdr.ChangeTrustResultCodeChangeTrustSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeChangeTrust, ChangeTrustResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("allow_trust_success", func(t *testing.T) {
		result := xdr.AllowTrustResult{Code: xdr.AllowTrustResultCodeAllowTrustSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeAllowTrust, AllowTrustResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("account_merge_success", func(t *testing.T) {
		result := xdr.AccountMergeResult{Code: xdr.AccountMergeResultCodeAccountMergeSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeAccountMerge, AccountMergeResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("inflation_success", func(t *testing.T) {
		result := xdr.InflationResult{Code: xdr.InflationResultCodeInflationSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeInflation, InflationResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("manage_data_success", func(t *testing.T) {
		result := xdr.ManageDataResult{Code: xdr.ManageDataResultCodeManageDataSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeManageData, ManageDataResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("bump_sequence_success", func(t *testing.T) {
		result := xdr.BumpSequenceResult{Code: xdr.BumpSequenceResultCodeBumpSequenceSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeBumpSequence, BumpSeqResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("path_payment_strict_send_success", func(t *testing.T) {
		result := xdr.PathPaymentStrictSendResult{Code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypePathPaymentStrictSend, PathPaymentStrictSendResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("create_claimable_balance_success", func(t *testing.T) {
		result := xdr.CreateClaimableBalanceResult{Code: xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeCreateClaimableBalance, CreateClaimableBalanceResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("claim_claimable_balance_success", func(t *testing.T) {
		result := xdr.ClaimClaimableBalanceResult{Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeClaimClaimableBalance, ClaimClaimableBalanceResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("begin_sponsoring_success", func(t *testing.T) {
		result := xdr.BeginSponsoringFutureReservesResult{Code: xdr.BeginSponsoringFutureReservesResultCodeBeginSponsoringFutureReservesSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeBeginSponsoringFutureReserves, BeginSponsoringFutureReservesResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("end_sponsoring_success", func(t *testing.T) {
		result := xdr.EndSponsoringFutureReservesResult{Code: xdr.EndSponsoringFutureReservesResultCodeEndSponsoringFutureReservesSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeEndSponsoringFutureReserves, EndSponsoringFutureReservesResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("revoke_sponsorship_success", func(t *testing.T) {
		result := xdr.RevokeSponsorshipResult{Code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeRevokeSponsorship, RevokeSponsorshipResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("clawback_success", func(t *testing.T) {
		result := xdr.ClawbackResult{Code: xdr.ClawbackResultCodeClawbackSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeClawback, ClawbackResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("clawback_claimable_balance_success", func(t *testing.T) {
		result := xdr.ClawbackClaimableBalanceResult{Code: xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeClawbackClaimableBalance, ClawbackClaimableBalanceResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("set_trust_line_flags_success", func(t *testing.T) {
		result := xdr.SetTrustLineFlagsResult{Code: xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeSetTrustLineFlags, SetTrustLineFlagsResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("liquidity_pool_deposit_success", func(t *testing.T) {
		result := xdr.LiquidityPoolDepositResult{Code: xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeLiquidityPoolDeposit, LiquidityPoolDepositResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("liquidity_pool_withdraw_success", func(t *testing.T) {
		result := xdr.LiquidityPoolWithdrawResult{Code: xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeLiquidityPoolWithdraw, LiquidityPoolWithdrawResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("invoke_host_function_success", func(t *testing.T) {
		result := xdr.InvokeHostFunctionResult{Code: xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeInvokeHostFunction, InvokeHostFunctionResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("extend_footprint_ttl_success", func(t *testing.T) {
		result := xdr.ExtendFootprintTtlResult{Code: xdr.ExtendFootprintTtlResultCodeExtendFootprintTtlSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeExtendFootprintTtl, ExtendFootprintTtlResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})

	t.Run("restore_footprint_success", func(t *testing.T) {
		result := xdr.RestoreFootprintResult{Code: xdr.RestoreFootprintResultCodeRestoreFootprintSuccess}
		tr := xdr.OperationResultTr{Type: xdr.OperationTypeRestoreFootprint, RestoreFootprintResult: &result}
		opr := xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}
		code, succ, err := forOperationResult(opr)
		require.NoError(t, err)
		assert.Equal(t, OpSuccess, code)
		assert.True(t, succ)
	})
}
