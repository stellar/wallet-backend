package utils

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/wallet-backend/internal/signing"
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

func SignatureClientResolver(signatureClientOpts *signing.SignatureClientOptions) (signing.SignatureClient, error) {
	signatureClient, err := signing.NewSignatureClient(signatureClientOpts)
	if err != nil {
		return nil, fmt.Errorf("instantiating signature client: %w", err)
	}
	return signatureClient, nil
}
