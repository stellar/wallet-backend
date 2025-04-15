package utils

import (
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/require"
)

// BuildTestTransaction is a test helper that builds a transaction with a random account.
// It is used to test the transaction manager.
// It is not used to build transactions for the TSS.
// For that, use the `BuildOperations` function.
func BuildTestTransaction(t *testing.T) *txnbuild.Transaction {
	t.Helper()

	accountToSponsor := keypair.MustRandom()

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: accountToSponsor.Address(),
			Sequence:  124,
		},
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			&txnbuild.Payment{
				Destination: keypair.MustRandom().Address(),
				Amount:      "14.0000000",
				Asset:       txnbuild.NativeAsset{},
			},
		},
		BaseFee:       104,
		Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
	})
	require.NoError(t, err)
	return tx
}

// BuildTestFeeBumpTransaction is a test helper that builds a fee bump transaction with a random fee account.
func BuildTestFeeBumpTransaction(t *testing.T) *txnbuild.FeeBumpTransaction {
	t.Helper()

	feeBumpTx, err := txnbuild.NewFeeBumpTransaction(
		txnbuild.FeeBumpTransactionParams{
			Inner:      BuildTestTransaction(t),
			FeeAccount: keypair.MustRandom().Address(),
			BaseFee:    110,
		})
	require.NoError(t, err)
	return feeBumpTx
}
