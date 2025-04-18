package utils

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"

	xdr3 "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

func OperationXDRToBase64(opXDR xdr.Operation) (string, error) {
	var buf strings.Builder
	enc := xdr3.NewEncoder(&buf)
	err := opXDR.EncodeTo(enc)
	if err != nil {
		return "", fmt.Errorf("encoding operation XDR with xdr3: %w", err)
	}

	opXDRStr := buf.String()
	return base64.StdEncoding.EncodeToString([]byte(opXDRStr)), nil
}

func OperationXDRFromBase64(opXDRBase64 string) (xdr.Operation, error) {
	opXDRBytes, err := base64.StdEncoding.DecodeString(opXDRBase64)
	if err != nil {
		return xdr.Operation{}, fmt.Errorf("decoding operation XDR base64: %w", err)
	}

	var opXDR xdr.Operation
	_, err = xdr3.Unmarshal(bytes.NewReader(opXDRBytes), &opXDR)
	if err != nil {
		return xdr.Operation{}, fmt.Errorf("unmarshalling operation XDR: %w", err)
	}
	return opXDR, nil
}

func OperationXDRToTxnBuildOp(opXDR xdr.Operation) (txnbuild.Operation, error) {
	var operation txnbuild.Operation

	switch xdr.OperationType(opXDR.Body.Type) {
	case xdr.OperationTypeCreateAccount:
		operation = &txnbuild.CreateAccount{}
	case xdr.OperationTypePayment:
		operation = &txnbuild.Payment{}
	case xdr.OperationTypePathPaymentStrictReceive:
		operation = &txnbuild.PathPaymentStrictReceive{}
	case xdr.OperationTypeManageSellOffer:
		operation = &txnbuild.ManageSellOffer{}
	case xdr.OperationTypeCreatePassiveSellOffer:
		operation = &txnbuild.CreatePassiveSellOffer{}
	case xdr.OperationTypeSetOptions:
		operation = &txnbuild.SetOptions{}
	case xdr.OperationTypeChangeTrust:
		operation = &txnbuild.ChangeTrust{}
	case xdr.OperationTypeAccountMerge:
		operation = &txnbuild.AccountMerge{}
	case xdr.OperationTypeInflation:
		operation = &txnbuild.Inflation{}
	case xdr.OperationTypeManageData:
		operation = &txnbuild.ManageData{}
	case xdr.OperationTypeBumpSequence:
		operation = &txnbuild.BumpSequence{}
	case xdr.OperationTypeManageBuyOffer:
		operation = &txnbuild.ManageBuyOffer{}
	case xdr.OperationTypePathPaymentStrictSend:
		operation = &txnbuild.PathPaymentStrictSend{}
	case xdr.OperationTypeCreateClaimableBalance:
		operation = &txnbuild.CreateClaimableBalance{}
	case xdr.OperationTypeClaimClaimableBalance:
		operation = &txnbuild.ClaimClaimableBalance{}
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		operation = &txnbuild.BeginSponsoringFutureReserves{}
	case xdr.OperationTypeEndSponsoringFutureReserves:
		operation = &txnbuild.EndSponsoringFutureReserves{}
	case xdr.OperationTypeRevokeSponsorship:
		operation = &txnbuild.RevokeSponsorship{}
	case xdr.OperationTypeClawback:
		operation = &txnbuild.Clawback{}
	case xdr.OperationTypeClawbackClaimableBalance:
		operation = &txnbuild.ClawbackClaimableBalance{}
	case xdr.OperationTypeSetTrustLineFlags:
		operation = &txnbuild.SetTrustLineFlags{}
	case xdr.OperationTypeAllowTrust:
		return nil, fmt.Errorf("the allow trust operation is deprecated, use SetTrustLineFlags instead")
	case xdr.OperationTypeLiquidityPoolDeposit:
		operation = &txnbuild.LiquidityPoolDeposit{}
	case xdr.OperationTypeLiquidityPoolWithdraw:
		operation = &txnbuild.LiquidityPoolWithdraw{}
	case xdr.OperationTypeInvokeHostFunction:
		operation = &txnbuild.InvokeHostFunction{}
	case xdr.OperationTypeExtendFootprintTtl:
		operation = &txnbuild.ExtendFootprintTtl{}
	case xdr.OperationTypeRestoreFootprint:
		operation = &txnbuild.RestoreFootprint{}
	default:
		return nil, fmt.Errorf("unrecognized operation type: %d", opXDR.Body.Type)
	}

	err := operation.FromXDR(opXDR)
	if err != nil {
		return nil, fmt.Errorf("decoding Operation FromXDR")
	}

	return operation, nil
}
