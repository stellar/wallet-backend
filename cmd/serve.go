package cmd

import (
	"fmt"
	"go/types"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/apptracker/sentry"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/serve"
	"github.com/stellar/wallet-backend/internal/signing"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
)

type serveCmd struct{}

func (c *serveCmd) Command() *cobra.Command {
	cfg := serve.Configs{}

	var sentryDSN string
	var stellarEnvironment string
	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&cfg.DatabaseURL),
		utils.LogLevelOption(&cfg.LogLevel),
		utils.NetworkPassphraseOption(&cfg.NetworkPassphrase),
		utils.BaseFeeOption(&cfg.BaseFee),
		utils.RPCURLOption(&cfg.RPCURL),
		utils.ChannelAccountEncryptionPassphraseOption(&cfg.EncryptionPassphrase),
		utils.SentryDSNOption(&sentryDSN),
		utils.StellarEnvironmentOption(&stellarEnvironment),
		utils.ServerBaseURLOption(&cfg.ServerBaseURL),
		utils.GraphQLComplexityLimitOption(&cfg.GraphQLComplexityLimit),
		utils.RedisHostOption(&cfg.RedisHost),
		utils.RedisPortOption(&cfg.RedisPort),
		{
			Name:        "port",
			Usage:       "Port to listen and serve on",
			OptType:     types.Int,
			ConfigKey:   &cfg.Port,
			FlagDefault: 8001,
			Required:    false,
		},
		{
			Name:           "client-auth-public-keys",
			Usage:          "A comma-separated list of public keys whose private keys are authorized to sign the payloads when making HTTP requests to this server. If not provided or empty, authentication is disabled.",
			OptType:        types.String,
			CustomSetValue: utils.SetConfigOptionStellarPublicKeyList,
			ConfigKey:      &cfg.ClientAuthPublicKeys,
			Required:       false,
		},
		{
			Name:        "client-auth-max-timeout-seconds",
			Usage:       "The maximum timeout for client authentication.",
			OptType:     types.Int,
			ConfigKey:   &cfg.ClientAuthMaxTimeoutSeconds,
			FlagDefault: 15,
			Required:    true,
		},
		{
			Name:        "client-auth-max-body-size-bytes",
			Usage:       "The maximum body size for client authentication, in bytes.",
			OptType:     types.Int,
			ConfigKey:   &cfg.ClientAuthMaxBodySizeBytes,
			FlagDefault: 102_400,
			Required:    true,
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
			FlagDefault: 15,
			Required:    true,
		},
		{
			Name:        "min-distribution-account-balance",
			Usage:       "Minimum XLM balance required for the distribution account in stroops (1 XLM = 10,000,000 stroops). Server will fail to start if balance is below this threshold. Set to 0 to only check account existence.",
			OptType:     types.Int,
			ConfigKey:   &cfg.MinDistributionAccountBalance,
			FlagDefault: 100_000_000, // 10 XLM in stroops
			Required:    false,
		},
	}

	// Distribution Account Signature Client options
	signatureClientOpts := utils.SignatureClientOptions{}
	cfgOpts = append(cfgOpts, utils.DistributionAccountSignatureProviderOption(&signatureClientOpts)...)

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

			signatureClientOpts.DBConnectionPool = dbConnectionPool
			signatureClientOpts.NetworkPassphrase = cfg.NetworkPassphrase
			signatureClient, err := utils.SignatureClientResolver(&signatureClientOpts)
			if err != nil {
				return fmt.Errorf("resolving distribution account signature client: %w", err)
			}
			cfg.DistributionAccountSignatureClient = signatureClient
			appTracker, err := sentry.NewSentryTracker(sentryDSN, stellarEnvironment, 5)
			if err != nil {
				return fmt.Errorf("initializing App Tracker: %w", err)
			}
			cfg.AppTracker = appTracker

			channelAccountSignatureClient, err := signing.NewChannelAccountDBSignatureClient(dbConnectionPool, cfg.NetworkPassphrase, &signingutils.DefaultPrivateKeyEncrypter{}, cfg.EncryptionPassphrase)
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
