package signing

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
)

var ErrInvalidTransaction = errors.New("invalid transaction provided")

type SignatureClient interface {
	NetworkPassphrase() string
	GetDistributionAccountPublicKey() string
	SignStellarTransaction(ctx context.Context, tx *txnbuild.Transaction) (*txnbuild.Transaction, error)
}

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

	if networkPassphrase != network.TestNetworkPassphrase && networkPassphrase != network.PublicNetworkPassphrase {
		return nil, fmt.Errorf("invalid network passphrase provided: %s", networkPassphrase)
	}

	return &envSignatureClient{
		networkPassphrase:       networkPassphrase,
		distributionAccountFull: distributionAccountFull,
	}, nil
}

func (sc *envSignatureClient) NetworkPassphrase() string {
	return sc.networkPassphrase
}

func (sc *envSignatureClient) GetDistributionAccountPublicKey() string {
	return sc.distributionAccountFull.Address()
}

func (sc *envSignatureClient) SignStellarTransaction(ctx context.Context, tx *txnbuild.Transaction) (*txnbuild.Transaction, error) {
	if tx == nil {
		return nil, ErrInvalidTransaction
	}

	signedTx, err := tx.Sign(sc.NetworkPassphrase(), sc.distributionAccountFull)
	if err != nil {
		return nil, fmt.Errorf("signing transaction: %w", err)
	}

	return signedTx, nil
}

func (sc envSignatureClient) String() string {
	return fmt.Sprintf("%T{networkPassphrase: %s, publicKey: %v}", sc, sc.networkPassphrase, sc.distributionAccountFull.Address())
}
