package signing

import (
	"context"
	"fmt"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvSignatureClientGetAccountPublicKey(t *testing.T) {
	ctx := context.Background()
	distributionAccount := keypair.MustRandom()
	sc, err := NewEnvSignatureClient(distributionAccount.Seed(), network.TestNetworkPassphrase)
	require.NoError(t, err)
	publicKey, err := sc.GetAccountPublicKey(ctx)
	require.NoError(t, err)
	assert.Equal(t, distributionAccount.Address(), publicKey)
}

func TestEnvSignatureClientNetworkPassphrase(t *testing.T) {
	distributionAccount := keypair.MustRandom()
	sc, err := NewEnvSignatureClient(distributionAccount.Seed(), network.TestNetworkPassphrase)
	require.NoError(t, err)
	assert.Equal(t, network.TestNetworkPassphrase, sc.NetworkPassphrase())
}

func TestEnvSignatureClientSignStellarTransaction(t *testing.T) {
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

	signedTx, err := sc.SignStellarTransaction(context.Background(), tx, distributionAccount.Address())
	require.NoError(t, err)
	assert.Equal(t, expectedSignedTx.Signatures(), signedTx.Signatures())

	signedTx, err = sc.SignStellarTransaction(context.Background(), nil)
	assert.ErrorIs(t, ErrInvalidTransaction, err)
	assert.Nil(t, signedTx)
}

func TestEnvSignatureClientSignStellarFeeBumpTransaction(t *testing.T) {
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

	feeBumpTx, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
		Inner:      tx,
		FeeAccount: distributionAccount.Address(),
		BaseFee:    txnbuild.MinBaseFee,
	})
	require.NoError(t, err)

	expectedSignedFeeBumpTx, err := feeBumpTx.Sign(network.TestNetworkPassphrase, distributionAccount)
	require.NoError(t, err)

	signedFeeBumpTx, err := sc.SignStellarFeeBumpTransaction(context.Background(), feeBumpTx)
	require.NoError(t, err)
	assert.Equal(t, expectedSignedFeeBumpTx.Signatures(), signedFeeBumpTx.Signatures())

	signedFeeBumpTx, err = sc.SignStellarFeeBumpTransaction(context.Background(), nil)
	assert.ErrorIs(t, ErrInvalidTransaction, err)
	assert.Nil(t, signedFeeBumpTx)
}

func TestEnvSignatureClientString(t *testing.T) {
	distributionAccount := keypair.MustRandom()
	signatureClient := envSignatureClient{
		distributionAccountFull: distributionAccount,
		networkPassphrase:       network.TestNetworkPassphrase,
	}

	testCases := []struct {
		name  string
		value string
	}{
		{name: "signatureClient.String()", value: signatureClient.String()},
		{name: "&signatureClient.String()", value: (&signatureClient).String()},
		{name: "%%v value", value: fmt.Sprintf("%v", signatureClient)},
		{name: "%%v pointer", value: fmt.Sprintf("%v", &signatureClient)},
		{name: "%%+v value", value: fmt.Sprintf("%+v", signatureClient)},
		{name: "%%+v pointer", value: fmt.Sprintf("%+v", &signatureClient)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.NotContains(t, tc.value, distributionAccount.Seed())
			assert.Contains(t, tc.value, distributionAccount.Address())
			assert.Contains(t, tc.value, network.TestNetworkPassphrase)
			assert.Contains(t, tc.value, fmt.Sprintf("%T", signatureClient))
		})
	}
}
