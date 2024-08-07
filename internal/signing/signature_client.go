package signing

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/signing/awskms"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/signing/utils"
)

var (
	ErrInvalidTransaction         = errors.New("invalid transaction provided")
	ErrNotImplemented             = errors.New("not implemented")
	ErrInvalidSignatureClientType = errors.New("invalid signature client type")
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

type SignatureClientOptions struct {
	Type                         SignatureClientType
	NetworkPassphrase            string
	DistributionAccountPublicKey string
	DBConnectionPool             db.ConnectionPool

	// Env Options
	DistributionAccountSecretKey string

	// AWS KMS
	KMSKeyARN string
	AWSRegion string

	// Channel Account
	EncryptionPassphrase string
}

func NewSignatureClient(opts *SignatureClientOptions) (SignatureClient, error) {
	switch opts.Type {
	case EnvSignatureClientType:
		return NewEnvSignatureClient(opts.DistributionAccountSecretKey, opts.NetworkPassphrase)
	case KMSSignatureClientType:
		kmsClient, err := awskms.GetKMSClient(opts.AWSRegion)
		if err != nil {
			return nil, fmt.Errorf("instantiating kms client: %w", err)
		}

		return NewKMSSignatureClient(
			opts.DistributionAccountPublicKey,
			opts.NetworkPassphrase,
			store.NewKeypairModel(opts.DBConnectionPool),
			kmsClient,
			opts.KMSKeyARN,
		)
	case ChannelAccountSignatureClientType:
		return NewChannelAccountDBSignatureClient(
			opts.DBConnectionPool,
			opts.NetworkPassphrase,
			&utils.DefaultPrivateKeyEncrypter{},
			opts.EncryptionPassphrase,
		)
	}

	return nil, ErrInvalidSignatureClientType
}
