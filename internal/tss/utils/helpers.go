package utils

import (
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
)

func BuildTestTransaction() *txnbuild.Transaction {
	accountToSponsor := keypair.MustRandom()

	tx, _ := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: accountToSponsor.Address(),
			Sequence:  124,
		},
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			&txnbuild.Payment{
				Destination: keypair.MustRandom().Address(),
				Amount:      "14",
				Asset:       txnbuild.NativeAsset{},
			},
		},
		BaseFee:       104,
		Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
	})
	return tx
}

func BuildTestFeeBumpTransaction() *txnbuild.FeeBumpTransaction {

	feeBumpTx, _ := txnbuild.NewFeeBumpTransaction(
		txnbuild.FeeBumpTransactionParams{
			Inner:      BuildTestTransaction(),
			FeeAccount: keypair.MustRandom().Address(),
			BaseFee:    110,
		})
	return feeBumpTx
}
