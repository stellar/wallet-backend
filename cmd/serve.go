package cmd

import (
	"go/types"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/serve"
)

type serveCmd struct{}

func (c *serveCmd) Command() *cobra.Command {
	cfg := serve.Configs{}
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
	}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run Wallet Backend server",
		PersistentPreRun: func(_ *cobra.Command, _ []string) {
			cfgOpts.Require()
			if err := cfgOpts.SetValues(); err != nil {
				log.Fatalf("Error setting values of config options: %s", err.Error())
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
