// Tests for operation result code extraction
// Verifies ForOperationResult correctly converts XDR operation results to string codes
package processors

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForOperationResult_OuterCodes(t *testing.T) {
	tests := []struct {
		name           string
		code           xdr.OperationResultCode
		expectedCode   string
		expectedSucc   bool
		expectError    bool
	}{
		{
			name:         "op_bad_auth",
			code:         xdr.OperationResultCodeOpBadAuth,
			expectedCode: "op_bad_auth",
			expectedSucc: false,
		},
		{
			name:         "op_no_source_account",
			code:         xdr.OperationResultCodeOpNoAccount,
			expectedCode: "op_no_source_account",
			expectedSucc: false,
		},
		{
			name:         "op_not_supported",
			code:         xdr.OperationResultCodeOpNotSupported,
			expectedCode: "op_not_supported",
			expectedSucc: false,
		},
		{
			name:         "op_too_many_subentries",
			code:         xdr.OperationResultCodeOpTooManySubentries,
			expectedCode: "op_too_many_subentries",
			expectedSucc: false,
		},
		{
			name:         "op_exceeded_work_limit",
			code:         xdr.OperationResultCodeOpExceededWorkLimit,
			expectedCode: "op_exceeded_work_limit",
			expectedSucc: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opr := xdr.OperationResult{
				Code: tt.code,
			}
			code, succ, err := ForOperationResult(opr)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedCode, code)
				assert.Equal(t, tt.expectedSucc, succ)
			}
		})
	}
}

func TestForOperationResult_PaymentSuccess(t *testing.T) {
	paymentResult := xdr.PaymentResult{
		Code: xdr.PaymentResultCodePaymentSuccess,
	}
	tr := xdr.OperationResultTr{
		Type:          xdr.OperationTypePayment,
		PaymentResult: &paymentResult,
	}
	opr := xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr:   &tr,
	}

	code, succ, err := ForOperationResult(opr)
	require.NoError(t, err)
	assert.Equal(t, OpSuccess, code)
	assert.True(t, succ)
}

func TestForOperationResult_PaymentUnderfunded(t *testing.T) {
	paymentResult := xdr.PaymentResult{
		Code: xdr.PaymentResultCodePaymentUnderfunded,
	}
	tr := xdr.OperationResultTr{
		Type:          xdr.OperationTypePayment,
		PaymentResult: &paymentResult,
	}
	opr := xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr:   &tr,
	}

	code, succ, err := ForOperationResult(opr)
	require.NoError(t, err)
	assert.Equal(t, OpUnderfunded, code)
	assert.False(t, succ)
}

func TestForOperationResult_CreateAccountSuccess(t *testing.T) {
	createAccountResult := xdr.CreateAccountResult{
		Code: xdr.CreateAccountResultCodeCreateAccountSuccess,
	}
	tr := xdr.OperationResultTr{
		Type:                xdr.OperationTypeCreateAccount,
		CreateAccountResult: &createAccountResult,
	}
	opr := xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr:   &tr,
	}

	code, succ, err := ForOperationResult(opr)
	require.NoError(t, err)
	assert.Equal(t, OpSuccess, code)
	assert.True(t, succ)
}

func TestForOperationResult_CreateAccountAlreadyExists(t *testing.T) {
	createAccountResult := xdr.CreateAccountResult{
		Code: xdr.CreateAccountResultCodeCreateAccountAlreadyExist,
	}
	tr := xdr.OperationResultTr{
		Type:                xdr.OperationTypeCreateAccount,
		CreateAccountResult: &createAccountResult,
	}
	opr := xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr:   &tr,
	}

	code, succ, err := ForOperationResult(opr)
	require.NoError(t, err)
	assert.Equal(t, "op_already_exists", code)
	assert.False(t, succ)
}

func TestForOperationResult_InvokeHostFunctionSuccess(t *testing.T) {
	invokeResult := xdr.InvokeHostFunctionResult{
		Code: xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess,
	}
	tr := xdr.OperationResultTr{
		Type:                     xdr.OperationTypeInvokeHostFunction,
		InvokeHostFunctionResult: &invokeResult,
	}
	opr := xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr:   &tr,
	}

	code, succ, err := ForOperationResult(opr)
	require.NoError(t, err)
	assert.Equal(t, OpSuccess, code)
	assert.True(t, succ)
}

func TestForOperationResult_InvokeHostFunctionTrapped(t *testing.T) {
	invokeResult := xdr.InvokeHostFunctionResult{
		Code: xdr.InvokeHostFunctionResultCodeInvokeHostFunctionTrapped,
	}
	tr := xdr.OperationResultTr{
		Type:                     xdr.OperationTypeInvokeHostFunction,
		InvokeHostFunctionResult: &invokeResult,
	}
	opr := xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr:   &tr,
	}

	code, succ, err := ForOperationResult(opr)
	require.NoError(t, err)
	assert.Equal(t, "function_trapped", code)
	assert.False(t, succ)
}

func TestForOperationResult_ChangeTrustSuccess(t *testing.T) {
	changeTrustResult := xdr.ChangeTrustResult{
		Code: xdr.ChangeTrustResultCodeChangeTrustSuccess,
	}
	tr := xdr.OperationResultTr{
		Type:              xdr.OperationTypeChangeTrust,
		ChangeTrustResult: &changeTrustResult,
	}
	opr := xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr:   &tr,
	}

	code, succ, err := ForOperationResult(opr)
	require.NoError(t, err)
	assert.Equal(t, OpSuccess, code)
	assert.True(t, succ)
}

func TestForOperationResult_ChangeTrustLowReserve(t *testing.T) {
	changeTrustResult := xdr.ChangeTrustResult{
		Code: xdr.ChangeTrustResultCodeChangeTrustLowReserve,
	}
	tr := xdr.OperationResultTr{
		Type:              xdr.OperationTypeChangeTrust,
		ChangeTrustResult: &changeTrustResult,
	}
	opr := xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr:   &tr,
	}

	code, succ, err := ForOperationResult(opr)
	require.NoError(t, err)
	assert.Equal(t, OpLowReserve, code)
	assert.False(t, succ)
}
