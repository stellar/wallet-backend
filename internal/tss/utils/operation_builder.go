package utils

import (
	"fmt"

	"github.com/stellar/go/txnbuild"

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

		// TODO: rethink this
		if !utils.IsSorobanXDROp(opXDR) && op.GetSourceAccount() == "" {
			return nil, fmt.Errorf("all Stellar Classic operations must have a source account explicitly set")
		}

		operations = append(operations, op)
	}

	return operations, nil
}
