package signing

import (
	"context"
	"errors"

	"github.com/stellar/go/txnbuild"
)

var (
	ErrInvalidTransaction = errors.New("invalid transaction provided")
	ErrNotImplemented     = errors.New("not implemented")
)

type SignatureClient interface {
	NetworkPassphrase() string
	GetAccountPublicKey(ctx context.Context) (string, error)
	SignStellarTransaction(ctx context.Context, tx *txnbuild.Transaction, stellarAccounts ...string) (*txnbuild.Transaction, error)
	SignStellarFeeBumpTransaction(ctx context.Context, feeBumpTx *txnbuild.FeeBumpTransaction) (*txnbuild.FeeBumpTransaction, error)
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
