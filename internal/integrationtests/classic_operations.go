package integrationtests

import (
	"fmt"

	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/pkg/utils"
)

// prepareClassicOps prepares a slice of strings, each representing a payment operation XDR. Currently only returns one operation (payment).
func (it *IntegrationTests) prepareClassicOps() ([]string, error) {
	paymentOp := &txnbuild.Payment{
		SourceAccount: it.SourceAccountKP.Address(),
		Destination:   it.SourceAccountKP.Address(),
		Amount:        "10",
		Asset:         txnbuild.NativeAsset{},
	}

	paymentOpXDR, err := paymentOp.BuildXDR()
	if err != nil {
		return nil, fmt.Errorf("building payment operation XDR: %w", err)
	}
	b64OpXDR, err := utils.OperationXDRToBase64(paymentOpXDR)
	if err != nil {
		return nil, fmt.Errorf("encoding payment operation XDR to base64: %w", err)
	}

	return []string{b64OpXDR}, nil
}
