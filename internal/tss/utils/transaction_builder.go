package utils

import (
	"bytes"
	"encoding/base64"
	"fmt"

	xdr3 "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

func BuildOriginalTransaction(txOpXDRs []string) (*txnbuild.Transaction, error) {
	var operations []txnbuild.Operation
	for _, opXDR := range txOpXDRs {
		decodedBytes, err := base64.StdEncoding.DecodeString(opXDR)
		if err != nil {
			return nil, fmt.Errorf("decoding Operation XDR string")
		}
		//dec := xdr3.NewDecoder(strings.NewReader(string(decodedBytes)))
		var decodedOp xdr.Operation
		//_, err = dec.Decode(&decodedOp)

		_, err = xdr3.Unmarshal(bytes.NewReader(decodedBytes), &decodedOp)

		if err != nil {
			return nil, fmt.Errorf("decoding xdr into xdr Operation: %w", err)
		}
		// for now, we assume that all operations are Payment operations
		paymentOp := txnbuild.Payment{}
		err = paymentOp.FromXDR(decodedOp)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling xdr into Operation: %w", err)
		}
		err = paymentOp.Validate()
		if err != nil {
			return nil, fmt.Errorf("invalid Operation: %w", err)
		}
		operations = append(operations, &paymentOp)
	}

	tx, _ := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: keypair.MustRandom().Address(),
		},
		//IncrementSequenceNum: true,
		Operations:    operations,
		BaseFee:       104,
		Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
	})
	return tx, nil
}
