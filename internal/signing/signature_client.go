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
