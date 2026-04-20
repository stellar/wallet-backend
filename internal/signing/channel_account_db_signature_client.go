package signing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/txnbuild"

	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
	"github.com/stellar/wallet-backend/internal/utils"
)

var ErrUnavailableChannelAccounts = errors.New("no available channel account could be found after retrying")

type channelAccountDBSignatureClient struct {
	networkPassphrase    string
	dbConnectionPool     *pgxpool.Pool
	encryptionPassphrase string
	privateKeyEncrypter  signingutils.PrivateKeyEncrypter
	channelAccountStore  store.ChannelAccountStore
	retryInterval        time.Duration
	retryCount           int
}

const (
	DefaultRetryInterval = 1 * time.Second
	DefaultRetryCount    = 6
)

var _ SignatureClient = (*channelAccountDBSignatureClient)(nil)

func NewChannelAccountDBSignatureClient(pool *pgxpool.Pool, networkPassphrase string, privateKeyEncrypter signingutils.PrivateKeyEncrypter, encryptionPassphrase string) (*channelAccountDBSignatureClient, error) {
	return &channelAccountDBSignatureClient{
		networkPassphrase:    networkPassphrase,
		dbConnectionPool:     pool,
		channelAccountStore:  store.NewChannelAccountModel(pool),
		privateKeyEncrypter:  privateKeyEncrypter,
		encryptionPassphrase: encryptionPassphrase,
		retryInterval:        DefaultRetryInterval,
		retryCount:           DefaultRetryCount,
	}, nil
}

func (sc *channelAccountDBSignatureClient) RetryInterval() time.Duration {
	if utils.IsEmpty(sc.retryInterval) {
		return DefaultRetryInterval
	}
	return sc.retryInterval
}

func (sc *channelAccountDBSignatureClient) RetryCount() int {
	if utils.IsEmpty(sc.retryCount) {
		return DefaultRetryCount
	}
	return sc.retryCount
}

func (sc *channelAccountDBSignatureClient) NetworkPassphrase() string {
	return sc.networkPassphrase
}

func (sc *channelAccountDBSignatureClient) GetAccountPublicKey(ctx context.Context, opts ...int) (string, error) {
	publicKey, _, err := sc.AcquireChannelAccount(ctx, opts...)
	return publicKey, err
}

// AcquireChannelAccount locks an idle channel account and returns its public key along with the locked_at
// timestamp that uniquely identifies this particular lock acquisition. The lockedAt value is the caller's
// lock token: passing it back to the store's UnlockChannelAccountByLockToken ensures a stale release can't
// wipe a newer lock on the same public_key if the original locked_until TTL expired and another request
// re-acquired the row first.
func (sc *channelAccountDBSignatureClient) AcquireChannelAccount(ctx context.Context, opts ...int) (string, time.Time, error) {
	var lockedUntil time.Duration
	if len(opts) > 0 {
		lockedUntil = time.Duration(opts[0]) * time.Second
	} else {
		lockedUntil = time.Minute
	}
	for range sc.RetryCount() {
		channelAccount, err := sc.channelAccountStore.GetAndLockIdleChannelAccount(ctx, lockedUntil)
		if err != nil {
			if errors.Is(err, store.ErrNoIdleChannelAccountAvailable) {
				log.Ctx(ctx).Warnf("All channel accounts are in use. Retry in %s.", sc.RetryInterval())
				time.Sleep(sc.RetryInterval())
				continue
			}

			return "", time.Time{}, fmt.Errorf("%w: total time spent waiting for an idle channel account: %v", ErrUnavailableChannelAccounts, time.Duration(sc.RetryCount())*sc.RetryInterval())
		}

		if !channelAccount.LockedAt.Valid {
			return "", time.Time{}, fmt.Errorf("channel account %s has no locked_at after acquisition; cannot form a lock token", channelAccount.PublicKey)
		}
		return channelAccount.PublicKey, channelAccount.LockedAt.Time, nil
	}

	numOfChannelAccounts, err := sc.channelAccountStore.Count(ctx)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("getting the number of channel accounts: %w", err)
	}

	if numOfChannelAccounts > 0 {
		return "", time.Time{}, store.ErrNoIdleChannelAccountAvailable
	}

	return "", time.Time{}, store.ErrNoChannelAccountConfigured
}

func (sc *channelAccountDBSignatureClient) getKPsForPublicKeys(ctx context.Context, stellarAccounts ...string) ([]*keypair.Full, error) {
	if len(stellarAccounts) == 0 {
		return nil, fmt.Errorf("no accounts provided")
	}

	channelAccounts, err := sc.channelAccountStore.GetAllByPublicKey(ctx, stellarAccounts...)
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
