package signing

import (
	"context"
	"fmt"
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

	signedTx, err := sc.SignStellarTransaction(context.Background(), tx)
	require.NoError(t, err)
	assert.Equal(t, expectedSignedTx.Signatures(), signedTx.Signatures())

	signedTx, err = sc.SignStellarTransaction(context.Background(), nil)
	assert.ErrorIs(t, ErrInvalidTransaction, err)
	assert.Nil(t, signedTx)
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

// TODO: remove this test - it's only to skip deadcode validation
func TestSignatureClientMock(t *testing.T) {
	sc := SignatureClientMock{}
	defer sc.AssertExpectations(t)

	ctx := context.Background()

	sc.On("NetworkPassphrase").Return(network.TestNetworkPassphrase)
	assert.Equal(t, network.TestNetworkPassphrase, sc.NetworkPassphrase())
	sc.On("GetDistributionAccountPublicKey").Return("pubkey")
	assert.Equal(t, "pubkey", sc.GetDistributionAccountPublicKey())
	sc.On("SignStellarTransaction", ctx, &txnbuild.Transaction{}).Return(nil, nil)
	tx, err := sc.SignStellarTransaction(ctx, &txnbuild.Transaction{})
	assert.Nil(t, tx)
	assert.NoError(t, err)
}
