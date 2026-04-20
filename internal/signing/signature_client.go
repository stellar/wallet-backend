package signing

import (
	"context"
	"errors"
	"time"

	"github.com/stellar/go-stellar-sdk/txnbuild"
)

var (
	ErrInvalidTransaction         = errors.New("invalid transaction provided")
	ErrNotImplemented             = errors.New("not implemented")
	ErrInvalidSignatureClientType = errors.New("invalid signature client type")
)

type SignatureClient interface {
	NetworkPassphrase() string
	GetAccountPublicKey(ctx context.Context, opts ...int) (string, error)
	SignStellarTransaction(ctx context.Context, tx *txnbuild.Transaction, stellarAccounts ...string) (*txnbuild.Transaction, error)
	SignStellarFeeBumpTransaction(ctx context.Context, feeBumpTx *txnbuild.FeeBumpTransaction) (*txnbuild.FeeBumpTransaction, error)
}

// ChannelAccountSignatureClient is a SignatureClient backed by a DB-managed pool of channel accounts, whose
// GetAccountPublicKey atomically locks a row for the duration of transaction building. Callers that need
// to safely release that lock on a failure path should use AcquireChannelAccount to obtain both the public
// key and the lockedAt token that identifies this specific acquisition — passing the token to the store's
// UnlockChannelAccountByLockToken guarantees the release only matches the caller's own lock and is a no-op
// if the TTL expired and another request already re-acquired the same row.
type ChannelAccountSignatureClient interface {
	SignatureClient
	AcquireChannelAccount(ctx context.Context, opts ...int) (publicKey string, lockedAt time.Time, err error)
}

type SignatureClientType string

const (
	EnvSignatureClientType            SignatureClientType = "ENV"
	KMSSignatureClientType            SignatureClientType = "KMS"
	ChannelAccountSignatureClientType SignatureClientType = "CHANNEL_ACCOUNT"
)

func (t SignatureClientType) IsValid() bool {
	switch t {
	case EnvSignatureClientType, KMSSignatureClientType, ChannelAccountSignatureClientType:
		return true
	default:
		return false
	}
}
