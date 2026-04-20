package cmd

import (
	"fmt"
	"go/types"

	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/apptracker/sentry"
	"github.com/stellar/wallet-backend/internal/serve"
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
		utils.RPCURLOption(&cfg.RPCURL),
		utils.SentryDSNOption(&sentryDSN),
		utils.StellarEnvironmentOption(&stellarEnvironment),
		utils.ServerBaseURLOption(&cfg.ServerBaseURL),
		utils.GraphQLComplexityLimitOption(&cfg.GraphQLComplexityLimit),
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
	}

	cfgOpts = append(cfgOpts, utils.DBPoolOptions(&cfg.DBMaxConns, &cfg.DBMinConns, &cfg.DBMaxConnLifetime, &cfg.DBMaxConnIdleTime)...)

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

			appTracker, err := sentry.NewSentryTracker(sentryDSN, stellarEnvironment, 5)
			if err != nil {
				return fmt.Errorf("initializing App Tracker: %w", err)
			}
			cfg.AppTracker = appTracker

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
