package utils

import (
	"bytes"
	"encoding/base64"
	"fmt"

	xdr3 "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/pkg/utils"
)

func BuildOperations(txOpXDRs []string) ([]txnbuild.Operation, error) {
	var operations []txnbuild.Operation
	for _, opStr := range txOpXDRs {
		opXDR, err := utils.OperationXDRFromBase64(opStr)
		if err != nil {
			return nil, fmt.Errorf("decoding Operation XDR string: %w", err)
		}

		op, err := utils.OperationXDRToTxnBuildOp(opXDR)
		if err != nil {
			return nil, fmt.Errorf("decoding Operation FromXDR")
		}

		if !utils.IsSorobanXDROp(opXDR) && op.GetSourceAccount() == "" {
			return nil, fmt.Errorf("all Stellar Classic operations must have a source account explicitly set")
		}

		operations = append(operations, op)
	}

	return operations, nil
}

func UnmarshallTransactionResultXDR(resultXDR string) (xdr.TransactionResult, error) {
	decodedBytes, err := base64.StdEncoding.DecodeString(resultXDR)
	if err != nil {
		return xdr.TransactionResult{}, fmt.Errorf("unable to decode errorResultXDR %s: %w", resultXDR, err)
	}
	var txResultXDR xdr.TransactionResult
	_, err = xdr3.Unmarshal(bytes.NewReader(decodedBytes), &txResultXDR)
	if err != nil {
		return xdr.TransactionResult{}, fmt.Errorf("unable to unmarshal errorResultXDR %s: %w", resultXDR, err)
	}
	return txResultXDR, nil
}
