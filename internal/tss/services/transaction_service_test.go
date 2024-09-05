package tss_services

import (
	"fmt"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
)

func TestThis(t *testing.T) {

	tx := buildTestTransaction()

	tsStr, _ := tx.Base64()
	oTx, err := txnbuild.TransactionFromXDR(tsStr)
	e := false
	if err != nil {
		e = true
	}
	assert.False(t, e)
	tt, _ := oTx.Transaction()
	fmt.Println("Base fee boo: ", tt.BaseFee())
}

func TestSendTransaction(t *testing.T) {
	txService, _ := NewTransactionService(TransactionServiceOptions{
		DistributionAccountSignatureClient: nil,
		ChannelAccountSignatureClient:      nil,
		HorizonClient:                      nil,
		RpcUrl:                             "http://localhost:8000/soroban/rpc",
		BaseFee:                            114,
	})
	txStr, _ := buildTestTransaction().Base64()
	txService.SendTransaction(txStr)

}

func buildTestTransaction() *txnbuild.Transaction {
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
		//BaseFee:       txnbuild.MinBaseFee,
		BaseFee:       104,
		Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
	})
	return tx

}
