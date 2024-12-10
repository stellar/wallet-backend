package utils

import (
	"bytes"
	"encoding/base64"
	"fmt"

	xdr3 "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

func BuildOperations(txOpXDRs []string) ([]txnbuild.Operation, error) {
	var operations []txnbuild.Operation
	for _, opStr := range txOpXDRs {
		decodedBytes, err := base64.StdEncoding.DecodeString(opStr)
		if err != nil {
			return nil, fmt.Errorf("decoding Operation XDR string")
		}
		var decodedOp xdr.Operation
		_, err = xdr3.Unmarshal(bytes.NewReader(decodedBytes), &decodedOp)
		if err != nil {
			return nil, fmt.Errorf("decoding xdr into xdr Operation: %w", err)
		}
		var operation txnbuild.Operation
		switch xdr.OperationType(decodedOp.Body.Type) {
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
			return nil, fmt.Errorf("unrecognized op")
		}
		err = operation.FromXDR(decodedOp)
		if err != nil {
			return nil, fmt.Errorf("decoding Operation FromXDR")
		}
		operations = append(operations, operation)
	}
	return operations, nil
}
