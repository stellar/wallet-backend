package integrationtests

import (
	"fmt"

	"github.com/stellar/go/txnbuild"
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
