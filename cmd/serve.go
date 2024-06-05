package cmd

import (
	"go/types"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/serve"
	"github.com/stellar/wallet-backend/internal/signing"
)

type serveCmd struct{}

func (c *serveCmd) Command() *cobra.Command {
	cfg := serve.Configs{}

	var (
		distributionAccountPrivateKey, horizonURL, networkPassphrase string
	)
	cfgOpts := config.ConfigOptions{
		{
			Name:        "port",
			Usage:       "Port to listen and serve on",
			OptType:     types.Int,
			ConfigKey:   &cfg.Port,
			FlagDefault: 8000,
			Required:    false,
		},
		{
			Name:        "database-url",
			Usage:       "Database connection URL",
			OptType:     types.String,
			ConfigKey:   &cfg.DatabaseURL,
			FlagDefault: "postgres://postgres@localhost:5432/wallet-backend?sslmode=disable",
			Required:    true,
		},
		{
			Name:        "server-base-url",
			Usage:       "The server base URL",
			OptType:     types.String,
			ConfigKey:   &cfg.ServerBaseURL,
			FlagDefault: "http://localhost:8000",
			Required:    true,
		},
		{
			Name:           "log-level",
			Usage:          `The log level used in this project. Options: "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", or "PANIC".`,
			OptType:        types.String,
			FlagDefault:    "TRACE",
			ConfigKey:      &cfg.LogLevel,
			CustomSetValue: utils.SetConfigOptionLogLevel,
			Required:       false,
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
			Name:           "assets",
			Usage:          `A collection of supported assets (i.e. USDC). This value is an array of JSON objects. Example: [{"code": "USDC", "issuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"}]`,
			OptType:        types.String,
			CustomSetValue: utils.SetConfigOptionAssets,
			ConfigKey:      &cfg.Assets,
			FlagDefault:    `[{"code": "USDC", "issuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"}, {"code": "ARST", "issuer": "GB7TAYRUZGE6TVT7NHP5SMIZRNQA6PLM423EYISAOAP3MKYIQMVYP2JO"}]`,
			Required:       true,
		},
		{
			Name:           "distribution-account-private-key",
			Usage:          "The Distribution Account private key.",
			OptType:        types.String,
			CustomSetValue: utils.SetConfigOptionStellarPrivateKey,
			ConfigKey:      &distributionAccountPrivateKey,
			Required:       true,
		},
		{
			Name:        "network-passphrase",
			Usage:       "The Stellar network passphrase",
			OptType:     types.String,
			ConfigKey:   &networkPassphrase,
			FlagDefault: network.TestNetworkPassphrase,
			Required:    true,
		},
		{
			Name:        "max-sponsored-threshold",
			Usage:       "The maximum reserves will be sponsored by the distribution account.",
			OptType:     types.Int,
			ConfigKey:   &cfg.MaxSponsoredThreshold,
			FlagDefault: 15,
			Required:    true,
		},
		{
			Name:        "base-fee",
			Usage:       "The base fee (in stroops) for submitting a Stellar transaction",
			OptType:     types.Int,
			ConfigKey:   &cfg.BaseFee,
			FlagDefault: 100 * txnbuild.MinBaseFee,
			Required:    true,
		},
		{
			Name:        "horizon-url",
			Usage:       "The URL of the Stellar Horizon server where this application will communicate with.",
			OptType:     types.String,
			ConfigKey:   &horizonURL,
			FlagDefault: horizonclient.DefaultTestNetClient.HorizonURL,
			Required:    true,
		},
	}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run Wallet Backend server",
		PersistentPreRun: func(_ *cobra.Command, _ []string) {
			cfgOpts.Require()
			if err := cfgOpts.SetValues(); err != nil {
				log.Fatalf("Error setting values of config options: %s", err.Error())
			}

			signatureClient, err := signing.NewEnvSignatureClient(distributionAccountPrivateKey, networkPassphrase)
			if err != nil {
				log.Fatalf("Error instantiating Env signature client: %s", err.Error())
			}

			cfg.SignatureClient = signatureClient
			cfg.HorizonClient = &horizonclient.Client{
				HorizonURL: horizonURL,
				HTTP:       &http.Client{Timeout: 40 * time.Second},
			}
		},
		Run: func(_ *cobra.Command, _ []string) {
			c.Run(cfg)
		},
	}
	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}
	return cmd
}

func (c *serveCmd) Run(cfg serve.Configs) {
	err := serve.Serve(cfg)
	if err != nil {
		log.Fatalf("Error running Serve: %s", err.Error())
	}
}
