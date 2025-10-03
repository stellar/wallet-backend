package signing

import (
	"context"
	"fmt"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
)

type envSignatureClient struct {
	networkPassphrase       string
	distributionAccountFull *keypair.Full
}

var _ SignatureClient = (*envSignatureClient)(nil)

func NewEnvSignatureClient(privateKey string, networkPassphrase string) (*envSignatureClient, error) {
	distributionAccountFull, err := keypair.ParseFull(privateKey)
	if err != nil {
		return nil, fmt.Errorf("parsing distribution account private key: %w", err)
	}

	return &envSignatureClient{
		networkPassphrase:       networkPassphrase,
		distributionAccountFull: distributionAccountFull,
	}, nil
}

func (sc *envSignatureClient) NetworkPassphrase() string {
	return sc.networkPassphrase
}

func (sc *envSignatureClient) GetAccountPublicKey(ctx context.Context, _ ...int) (string, error) {
	return sc.distributionAccountFull.Address(), nil
}

func (sc *envSignatureClient) SignStellarTransaction(ctx context.Context, tx *txnbuild.Transaction, stellarAccounts ...string) (*txnbuild.Transaction, error) {
	if tx == nil {
		return nil, ErrInvalidTransaction
	}

	if len(stellarAccounts) == 0 {
		return nil, fmt.Errorf("stellar accounts cannot be empty in %T", sc)
	}

	// Ensure that the distribution account is the only account signing the transaction
	for _, stellarAccount := range stellarAccounts {
		if stellarAccount != sc.distributionAccountFull.Address() {
			return nil, fmt.Errorf("stellar account %s is not allowed to sign in %T", stellarAccount, sc)
		}
	}

	signedTx, err := tx.Sign(sc.NetworkPassphrase(), sc.distributionAccountFull)
	if err != nil {
		return nil, fmt.Errorf("signing transaction in %T: %w", sc, err)
	}

	return signedTx, nil
}

func (sc *envSignatureClient) SignStellarFeeBumpTransaction(ctx context.Context, feeBumpTx *txnbuild.FeeBumpTransaction) (*txnbuild.FeeBumpTransaction, error) {
	if feeBumpTx == nil {
		return nil, ErrInvalidTransaction
	}

	signedFeeBumpTx, err := feeBumpTx.Sign(sc.NetworkPassphrase(), sc.distributionAccountFull)
	if err != nil {
		return nil, fmt.Errorf("signing fee bump transaction: %w", err)
	}

	return signedFeeBumpTx, nil
}

func (sc envSignatureClient) String() string {
	return fmt.Sprintf("%T{networkPassphrase: %s, publicKey: %v}", sc, sc.networkPassphrase, sc.distributionAccountFull.Address())
}
