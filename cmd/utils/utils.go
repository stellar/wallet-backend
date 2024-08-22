package utils

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/awskms"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/signing/utils"
)

func DefaultPersistentPreRunE(cfgOpts config.ConfigOptions) func(_ *cobra.Command, _ []string) error {
	return func(_ *cobra.Command, _ []string) error {
		if err := cfgOpts.RequireE(); err != nil {
			return fmt.Errorf("requiring values of config options: %w", err)
		}
		if err := cfgOpts.SetValues(); err != nil {
			return fmt.Errorf("setting values of config options: %w", err)
		}
		return nil
	}
}

type SignatureClientOptions struct {
	Type                         signing.SignatureClientType
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

func SignatureClientResolver(signatureClientOpts *SignatureClientOptions) (signing.SignatureClient, error) {
	switch signatureClientOpts.Type {
	case signing.EnvSignatureClientType:
		return signing.NewEnvSignatureClient(signatureClientOpts.DistributionAccountSecretKey, signatureClientOpts.NetworkPassphrase)
	case signing.KMSSignatureClientType:
		kmsClient, err := awskms.GetKMSClient(signatureClientOpts.AWSRegion)
		if err != nil {
			return nil, fmt.Errorf("instantiating kms client: %w", err)
		}

		return signing.NewKMSSignatureClient(
			signatureClientOpts.DistributionAccountPublicKey,
			signatureClientOpts.NetworkPassphrase,
			store.NewKeypairModel(signatureClientOpts.DBConnectionPool),
			kmsClient,
			signatureClientOpts.KMSKeyARN,
		)
	case signing.ChannelAccountSignatureClientType:
		return signing.NewChannelAccountDBSignatureClient(
			signatureClientOpts.DBConnectionPool,
			signatureClientOpts.NetworkPassphrase,
			&utils.DefaultPrivateKeyEncrypter{},
			signatureClientOpts.EncryptionPassphrase,
		)
	}

	return nil, signing.ErrInvalidSignatureClientType
}
