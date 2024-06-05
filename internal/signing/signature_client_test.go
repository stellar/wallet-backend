package signing

import (
	"context"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvSignatureClientGetDistributionAccountPublicKey(t *testing.T) {
	distributionAccount := keypair.MustRandom()
	sc, err := NewEnvSignatureClient(distributionAccount.Seed(), network.TestNetworkPassphrase)
	require.NoError(t, err)
	assert.Equal(t, distributionAccount.Address(), sc.GetDistributionAccountPublicKey())
}

func TestEnvSignatureClientNetworkPassphrase(t *testing.T) {
	distributionAccount := keypair.MustRandom()
	sc, err := NewEnvSignatureClient(distributionAccount.Seed(), network.TestNetworkPassphrase)
	require.NoError(t, err)
	assert.Equal(t, network.TestNetworkPassphrase, sc.NetworkPassphrase())
}

func TestEnvSignatureSignStellarTransaction(t *testing.T) {
	distributionAccount := keypair.MustRandom()
	sc, err := NewEnvSignatureClient(distributionAccount.Seed(), network.TestNetworkPassphrase)
	require.NoError(t, err)

	sourceAccount := txnbuild.NewSimpleAccount(distributionAccount.Address(), int64(9605939170639897))
	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount:        &sourceAccount,
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: "GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z",
					Amount:      "10",
					Asset:       txnbuild.NativeAsset{},
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
		},
	)
	require.NoError(t, err)

	expectedSignedTx, err := tx.Sign(network.TestNetworkPassphrase, distributionAccount)
	require.NoError(t, err)

	signedTx, err := sc.SignStellarTransaction(context.Background(), tx)
	require.NoError(t, err)
	assert.Equal(t, expectedSignedTx.Signatures(), signedTx.Signatures())

	signedTx, err = sc.SignStellarTransaction(context.Background(), nil)
	assert.ErrorIs(t, ErrInvalidTransaction, err)
	assert.Nil(t, signedTx)
}
