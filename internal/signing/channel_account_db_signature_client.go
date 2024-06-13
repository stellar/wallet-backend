package signing

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/signing/channelaccounts"
)

type channelAccountDBSignatureClient struct {
	networkPassphrase    string
	dbConnectionPool     db.ConnectionPool
	encryptionPassphrase string
	privateKeyEncrypter  channelaccounts.PrivateKeyEncrypter
	channelAccountStore  channelaccounts.ChannelAccountStore
}

var _ SignatureClient = (*channelAccountDBSignatureClient)(nil)

func NewChannelAccountDBSignatureClient(dbConnectionPool db.ConnectionPool, networkPassphrase string, privateKeyEncrypter channelaccounts.PrivateKeyEncrypter, encryptionPassphrase string) (*channelAccountDBSignatureClient, error) {
	if networkPassphrase != network.TestNetworkPassphrase && networkPassphrase != network.PublicNetworkPassphrase {
		return nil, fmt.Errorf("invalid network passphrase provided: %s", networkPassphrase)
	}

	return &channelAccountDBSignatureClient{
		networkPassphrase:    networkPassphrase,
		dbConnectionPool:     dbConnectionPool,
		channelAccountStore:  channelaccounts.NewChannelAccountModel(dbConnectionPool),
		privateKeyEncrypter:  privateKeyEncrypter,
		encryptionPassphrase: encryptionPassphrase,
	}, nil
}

func (sc *channelAccountDBSignatureClient) NetworkPassphrase() string {
	return sc.networkPassphrase
}

func (sc *channelAccountDBSignatureClient) GetAccountPublicKey(ctx context.Context) (string, error) {
	channelAccount, err := sc.channelAccountStore.GetIdleChannelAccount(ctx, time.Minute)
	if err != nil {
		return "", fmt.Errorf("getting idle channel account: %w", err)
	}
	return channelAccount.PublicKey, nil
}

func (sc *channelAccountDBSignatureClient) getKPsForPublicKeys(ctx context.Context, stellarAccounts ...string) ([]*keypair.Full, error) {
	if len(stellarAccounts) == 0 {
		return nil, fmt.Errorf("no accounts provided")
	}

	accountsAlreadyAccountedFor := map[string]struct{}{}
	kps := []*keypair.Full{}
	for i, account := range stellarAccounts {
		if _, ok := accountsAlreadyAccountedFor[account]; ok {
			continue
		}
		accountsAlreadyAccountedFor[account] = struct{}{}

		if account == "" {
			return nil, fmt.Errorf("account %d is empty", i)
		}

		chAcc, err := sc.channelAccountStore.Get(ctx, sc.dbConnectionPool, account)
		if err != nil {
			return nil, fmt.Errorf("getting secret for channel account %q: %w", account, err)
		}

		chAccPrivateKey, err := sc.privateKeyEncrypter.Decrypt(ctx, chAcc.EncryptedPrivateKey, sc.encryptionPassphrase)
		if err != nil {
			return nil, fmt.Errorf("cannot decrypt private key: %w", err)
		}

		kp, err := keypair.ParseFull(chAccPrivateKey)
		if err != nil {
			return nil, fmt.Errorf("parsing secret for channel account %q: %w", account, err)
		}
		kps = append(kps, kp)
	}

	return kps, nil
}

func (sc *channelAccountDBSignatureClient) SignStellarTransaction(ctx context.Context, tx *txnbuild.Transaction, stellarAccounts ...string) (*txnbuild.Transaction, error) {
	if tx == nil {
		return nil, ErrInvalidTransaction
	}

	kps, err := sc.getKPsForPublicKeys(ctx, stellarAccounts...)
	if err != nil {
		return nil, fmt.Errorf("getting keypairs for accounts %v in %T: %w", stellarAccounts, sc, err)
	}

	signedTx, err := tx.Sign(sc.NetworkPassphrase(), kps...)
	if err != nil {
		return nil, fmt.Errorf("signing transaction in %T: %w", sc, err)
	}

	return signedTx, nil
}

func (sc *channelAccountDBSignatureClient) SignStellarFeeBumpTransaction(ctx context.Context, feeBumpTx *txnbuild.FeeBumpTransaction) (*txnbuild.FeeBumpTransaction, error) {
	return nil, ErrNotImplemented
}
