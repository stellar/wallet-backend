package utils

import (
	"encoding/base64"
	"strings"
	"testing"

	xdr3 "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
)

func TestBuildOriginalTransaction(t *testing.T) {
	t.Run("return_error_when_unable_to_decode_operation_xdr_string", func(t *testing.T) {
		_, err := BuildOriginalTransaction([]string{"this@is#not$valid!"})
		assert.Equal(t, "decoding Operation XDR string", err.Error())

	})
	t.Run("return_error_when_unable_to_unmarshal_xdr_into_operation", func(t *testing.T) {
		ca := txnbuild.CreateAccount{
			Destination:   keypair.MustRandom().Address(),
			Amount:        "10",
			SourceAccount: keypair.MustRandom().Address(),
		}
		caOp, _ := ca.BuildXDR()

		buf := strings.Builder{}
		enc := xdr3.NewEncoder(&buf)
		caOp.EncodeTo(enc)

		caOpXDR := buf.String()
		caOpXDRBase64 := base64.StdEncoding.EncodeToString([]byte(caOpXDR))

		_, err := BuildOriginalTransaction([]string{caOpXDRBase64})
		assert.Equal(t, "unmarshaling xdr into Operation: error parsing payment operation from xdr", err.Error())
	})

	t.Run("return_expected_transaction", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		p := txnbuild.Payment{
			Destination:   keypair.MustRandom().Address(),
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: srcAccount,
		}
		op, _ := p.BuildXDR()

		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		op.EncodeTo(enc)

		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))
		tx, err := BuildOriginalTransaction([]string{opXDRBase64})
		firstOp := tx.Operations()[0]
		assert.Equal(t, firstOp.GetSourceAccount(), srcAccount)
		assert.Empty(t, err)
	})

}
