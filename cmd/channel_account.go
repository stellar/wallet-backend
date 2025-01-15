package cmd

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
)

type channelAccountCmdConfigOptions struct {
	DatabaseURL                   string
	NetworkPassphrase             string
	BaseFee                       int
	DistributionAccountPrivateKey string
	EncryptionPassphrase          string
}

type channelAccountCmd struct {
	channelAccountService services.ChannelAccountService
}

func (c *channelAccountCmd) Command() *cobra.Command {
	cfg := channelAccountCmdConfigOptions{}
	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&cfg.DatabaseURL),
		utils.NetworkPassphraseOption(&cfg.NetworkPassphrase),
		utils.BaseFeeOption(&cfg.BaseFee),
		utils.ChannelAccountEncryptionPassphraseOption(&cfg.EncryptionPassphrase),
	}

	// Distribution Account Signature Client options
	signatureClientOpts := utils.SignatureClientOptions{}
	cfgOpts = append(cfgOpts, utils.DistributionAccountSignatureProviderOption(&signatureClientOpts)...)

	cmd := &cobra.Command{
		Use:               "channel-account",
		Short:             "Manage channel accounts",
		PersistentPreRunE: utils.DefaultPersistentPreRunE(cfgOpts),
	}

	ensureCmd := &cobra.Command{
		Use:   "ensure",
		Short: "Ensures if the [number] of channel accounts are created",
		Args:  cobra.ExactArgs(1),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}

			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}

			dbConnectionPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL, nil)
			if err != nil {
				return fmt.Errorf("opening connection pool: %w", err)
			}

			signatureClientOpts.DBConnectionPool = dbConnectionPool
			signatureClientOpts.NetworkPassphrase = cfg.NetworkPassphrase
			signatureClient, err := utils.SignatureClientResolver(&signatureClientOpts)
			if err != nil {
				return fmt.Errorf("resolving distribution account signature client: %w", err)
			}

			channelAccountModel := store.ChannelAccountModel{DB: dbConnectionPool}
			privateKeyEncrypter := signingutils.DefaultPrivateKeyEncrypter{}
			c.channelAccountService, err = services.NewChannelAccountService(services.ChannelAccountServiceOptions{
				DB:                                 dbConnectionPool,
				BaseFee:                            int64(cfg.BaseFee),
				DistributionAccountSignatureClient: signatureClient,
				ChannelAccountStore:                &channelAccountModel,
				PrivateKeyEncrypter:                &privateKeyEncrypter,
				EncryptionPassphrase:               cfg.EncryptionPassphrase,
			})
			if err != nil {
				return fmt.Errorf("instantiating channel account services: %w", err)
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			count, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid [number] argument: %s", args[0])
			}

			if err = c.channelAccountService.EnsureChannelAccounts(cmd.Context(), int64(count)); err != nil {
				return fmt.Errorf("ensuring the number of channel accounts is created: %w", err)
			}

			return nil
		},
	}

	cmd.AddCommand(ensureCmd)

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	return cmd
}
