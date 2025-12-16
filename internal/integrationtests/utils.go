package integrationtests

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/txnbuild"
)

func parseTxXDR(txXDR string) (*txnbuild.Transaction, error) {
	genericTx, err := txnbuild.TransactionFromXDR(txXDR)
	if err != nil {
		return nil, fmt.Errorf("building transaction from XDR: %w", err)
	}

	tx, ok := genericTx.Transaction()
	if !ok {
		return nil, fmt.Errorf("genericTx must be a transaction")
	}
	return tx, nil
}

func parseFeeBumpTxXDR(txXDR string) (*txnbuild.FeeBumpTransaction, error) {
	genericTx, err := txnbuild.TransactionFromXDR(txXDR)
	if err != nil {
		return nil, fmt.Errorf("building transaction from XDR: %w", err)
	}

	feeBumpTx, ok := genericTx.FeeBump()
	if !ok {
		return nil, fmt.Errorf("genericTx must be a fee bump transaction")
	}
	return feeBumpTx, nil
}

// txString returns a string representation of a transaction given its XDR.
func txString(txXDR string) (string, error) {
	genericTx, err := txnbuild.TransactionFromXDR(txXDR)
	if err != nil {
		return "", fmt.Errorf("building transaction from XDR: %w", err)
	}

	opsSliceStr := func(ops []txnbuild.Operation) []string {
		var opsStr []string
		for _, op := range ops {
			opsStr = append(opsStr, fmt.Sprintf("\n\t\t%#v", op))
		}
		return opsStr
	}

	if tx, ok := genericTx.Transaction(); ok {
		return fmt.Sprintf("\n\ttx=%#v, \n\tops=%+v", tx, opsSliceStr(tx.Operations())), nil
	} else if feeBumpTx, ok := genericTx.FeeBump(); ok {
		return fmt.Sprintf("\n\tfeeBump=%#v, \n\ttx=%#v, \n\tops=%+v", feeBumpTx, feeBumpTx.InnerTransaction(), opsSliceStr(feeBumpTx.InnerTransaction().Operations())), nil
	}

	return fmt.Sprintf("%+v", genericTx), nil
}
