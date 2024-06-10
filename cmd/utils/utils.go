package utils

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
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
