package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing/awskms"
	"github.com/stellar/wallet-backend/internal/signing/store"
)

type kmsCommandConfig struct {
	databaseURL                  string
	kmsKeyARN                    string
	awsRegion                    string
	distributionAccountPublicKey string
}

type distributionAccountCmd struct{}

func (c *distributionAccountCmd) Command() *cobra.Command {
	cmd := cobra.Command{
		Use:   "distribution-account",
		Short: "Distribution Account Private Key management.",
	}

	cmd.AddCommand(kmsCommand())

	return &cmd
}

func kmsCommand() *cobra.Command {
	cfg := kmsCommandConfig{}
	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&cfg.databaseURL),
		utils.DistributionAccountPublicKeyOption(&cfg.distributionAccountPublicKey),
	}
	cfgOpts = append(cfgOpts, utils.AWSOptions(&cfg.awsRegion, &cfg.kmsKeyARN, true)...)

	cmd := &cobra.Command{
		Use:   "kms",
		Short: "Manage the Distribution Account private key using KMS.",
	}

	var kmsImportService services.KMSImportService
	importCmd := &cobra.Command{
		Use:   "import",
		Short: "Import your Distribution Account Private Key. This command encrypts and stores the encrypted private key.",
		Args:  cobra.NoArgs,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}

			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}

			dbConnectionPool, err := db.OpenDBConnectionPool(cfg.databaseURL)
			if err != nil {
				return fmt.Errorf("opening connection pool: %w", err)
			}

			kmsClient, err := awskms.GetKMSClient(cfg.awsRegion)
			if err != nil {
				return fmt.Errorf("getting kms client: %w", err)
			}

			kmsImportService, err = services.NewKMSImportService(kmsClient, cfg.kmsKeyARN, store.NewKeypairModel(dbConnectionPool), cfg.distributionAccountPublicKey)
			if err != nil {
				return fmt.Errorf("instantiating kms import service: %w", err)
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			passwordPrompter, err := utils.NewDefaultPasswordPrompter(
				"🔑 Input your Distribution Account Private Key (key will be hidden):", os.Stdin, os.Stdout)
			if err != nil {
				return fmt.Errorf("instantiating password prompter: %w", err)
			}

			distributionAccountSeed, err := passwordPrompter.Run()
			if err != nil {
				return fmt.Errorf("getting distribution account seed input: %w", err)
			}

			err = kmsImportService.ImportDistributionAccountKey(ctx, distributionAccountSeed)
			if err != nil {
				if errors.Is(err, services.ErrMismatchDistributionAccount) {
					return fmt.Errorf("the private key provided doesn't belong to the configured distribution account public key")
				}
				return fmt.Errorf("importing distribution account seed: %w", err)
			}

			log.Ctx(ctx).Info("Successfully imported and encrypted the Distribution Account Private Key")
			return nil
		},
	}

	cmd.AddCommand(importCmd)

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	return cmd
}
