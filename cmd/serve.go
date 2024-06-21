package cmd

import (
	"fmt"
	"go/types"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/serve"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/channelaccounts"
)

type serveCmd struct{}

func (c *serveCmd) Command() *cobra.Command {
	cfg := serve.Configs{}

	var distributionAccountPrivateKey string
	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&cfg.DatabaseURL),
		utils.LogLevelOption(&cfg.LogLevel),
		utils.NetworkPassphraseOption(&cfg.NetworkPassphrase),
		utils.BaseFeeOption(&cfg.BaseFee),
		utils.HorizonClientURLOption(&cfg.HorizonClientURL),
		utils.DistributionAccountPrivateKeyOption(&distributionAccountPrivateKey),
		utils.ChannelAccountEncryptionPassphraseOption(&cfg.EncryptionPassphrase),
		{
			Name:        "port",
			Usage:       "Port to listen and serve on",
			OptType:     types.Int,
			ConfigKey:   &cfg.Port,
			FlagDefault: 8001,
			Required:    false,
		},
		{
			Name:        "server-base-url",
			Usage:       "The server base URL",
			OptType:     types.String,
			ConfigKey:   &cfg.ServerBaseURL,
			FlagDefault: "http://localhost:8001",
			Required:    true,
		},
		{
			Name:           "wallet-signing-key",
			Usage:          "The public key of the Stellar account that signs the payloads when making HTTP Request to this server.",
			OptType:        types.String,
			CustomSetValue: utils.SetConfigOptionStellarPublicKey,
			ConfigKey:      &cfg.WalletSigningKey,
			Required:       true,
		},
		{
			Name:           "supported-assets",
			Usage:          `A collection of supported assets (i.e. USDC). This value is an array of JSON objects. Example: [{"code": "USDC", "issuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"}]`,
			OptType:        types.String,
			CustomSetValue: utils.SetConfigOptionAssets,
			ConfigKey:      &cfg.SupportedAssets,
			FlagDefault:    `[{"code": "USDC", "issuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"}, {"code": "ARST", "issuer": "GB7TAYRUZGE6TVT7NHP5SMIZRNQA6PLM423EYISAOAP3MKYIQMVYP2JO"}]`,
			Required:       true,
		},
		{
			Name:        "max-sponsored-base-reservers",
			Usage:       "The maximum reserves will be sponsored by the distribution account.",
			OptType:     types.Int,
			ConfigKey:   &cfg.MaxSponsoredBaseReserves,
			FlagDefault: 15,
			Required:    true,
		},
		{
			Name:        "number-channel-accounts",
			Usage:       "The minimum number of Channel Accounts that must exist in the database.",
			OptType:     types.Int,
			ConfigKey:   &cfg.NumberOfChannelAccounts,
			FlagDefault: 5,
			Required:    true,
		},
	}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run Wallet Backend server",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}
			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}

			dbConnectionPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL)
			if err != nil {
				return fmt.Errorf("opening connection pool: %w", err)
			}

			signatureClient, err := signing.NewEnvSignatureClient(distributionAccountPrivateKey, cfg.NetworkPassphrase)
			if err != nil {
				return fmt.Errorf("instantiating env signature client: %w", err)
			}
			cfg.DistributionAccountSignatureClient = signatureClient

			channelAccountSignatureClient, err := signing.NewChannelAccountDBSignatureClient(dbConnectionPool, cfg.NetworkPassphrase, &channelaccounts.DefaultPrivateKeyEncrypter{}, distributionAccountPrivateKey)
			if err != nil {
				return fmt.Errorf("instantiating channel account db signature client: %w", err)
			}
			cfg.ChannelAccountSignatureClient = channelAccountSignatureClient

			return nil
		},
		RunE: func(_ *cobra.Command, _ []string) error {
			return c.Run(cfg)
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	return cmd
}

func (c *serveCmd) Run(cfg serve.Configs) error {
	err := serve.Serve(cfg)
	if err != nil {
		return fmt.Errorf("running serve: %w", err)
	}
	return nil
}
