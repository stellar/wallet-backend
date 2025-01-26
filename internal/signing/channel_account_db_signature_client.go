package signing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
)

type channelAccountDBSignatureClient struct {
	networkPassphrase    string
	dbConnectionPool     db.ConnectionPool
	encryptionPassphrase string
	privateKeyEncrypter  signingutils.PrivateKeyEncrypter
	channelAccountStore  store.ChannelAccountStore
}

var _ SignatureClient = (*channelAccountDBSignatureClient)(nil)

func NewChannelAccountDBSignatureClient(dbConnectionPool db.ConnectionPool, networkPassphrase string, privateKeyEncrypter signingutils.PrivateKeyEncrypter, encryptionPassphrase string) (*channelAccountDBSignatureClient, error) {
	if networkPassphrase != network.TestNetworkPassphrase && networkPassphrase != network.PublicNetworkPassphrase {
		return nil, fmt.Errorf("invalid network passphrase provided: %s", networkPassphrase)
	}

	return &channelAccountDBSignatureClient{
		networkPassphrase:    networkPassphrase,
		dbConnectionPool:     dbConnectionPool,
		channelAccountStore:  store.NewChannelAccountModel(dbConnectionPool),
		privateKeyEncrypter:  privateKeyEncrypter,
		encryptionPassphrase: encryptionPassphrase,
	}, nil
}

func (sc *channelAccountDBSignatureClient) NetworkPassphrase() string {
	return sc.networkPassphrase
}

func (sc *channelAccountDBSignatureClient) GetAccountPublicKey(ctx context.Context, opts ...int) (string, error) {
	var lockedUntil time.Duration
	if len(opts) > 0 {
		lockedUntil = time.Duration(opts[0]) * time.Second
	} else {
		lockedUntil = time.Minute
	}
	for range store.ChannelAccountWaitTime {
		// check to see if the variadic parameter for time exists and if so, use it here
		channelAccount, err := sc.channelAccountStore.GetIdleChannelAccount(ctx, lockedUntil)
		if err != nil {
			if errors.Is(err, store.ErrNoIdleChannelAccountAvailable) {
				log.Ctx(ctx).Warn("All channel accounts are in use. Retry in 1 second.")
				time.Sleep(1 * time.Second)
				continue
			}
			return "", fmt.Errorf("getting idle channel account: %w", err)
		}

		return channelAccount.PublicKey, nil
	}

	numOfChannelAccounts, err := sc.channelAccountStore.Count(ctx)
	if err != nil {
		return "", fmt.Errorf("getting the number of channel accounts: %w", err)
	}

	if numOfChannelAccounts > 0 {
		return "", store.ErrNoIdleChannelAccountAvailable
	}

	return "", store.ErrNoChannelAccountConfigured
}

func (sc *channelAccountDBSignatureClient) getKPsForPublicKeys(ctx context.Context, stellarAccounts ...string) ([]*keypair.Full, error) {
	if len(stellarAccounts) == 0 {
		return nil, fmt.Errorf("no accounts provided")
	}

	channelAccounts, err := sc.channelAccountStore.GetAllByPublicKey(ctx, sc.dbConnectionPool, stellarAccounts...)
	if err != nil {
		return nil, fmt.Errorf("getting channel accounts %v: %w", stellarAccounts, err)
	}

	channelAccountsMap := make(map[string]*store.ChannelAccount, len(channelAccounts))
	for _, chAcc := range channelAccounts {
		channelAccountsMap[chAcc.PublicKey] = chAcc
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

		chAcc, ok := channelAccountsMap[account]
		if !ok {
			return nil, fmt.Errorf("account %s not found", account)
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
