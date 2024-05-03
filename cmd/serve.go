package cmd

import (
	"go/types"
	"wallet-backend/internal/serve"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	supportlog "github.com/stellar/go/support/log"
)

type serveCmd struct {
	Logger *supportlog.Entry
}

func (c *serveCmd) Command() *cobra.Command {
	cfg := serve.Configs{
		Logger: c.Logger,
	}
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
	}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run Wallet Backend server",
		Run: func(_ *cobra.Command, _ []string) {
			cfgOpts.Require()
			if err := cfgOpts.SetValues(); err != nil {
				c.Logger.Fatalf("Error setting values of config options: %s", err.Error())
			}
			c.Run(cfg)
		},
	}
	if err := cfgOpts.Init(cmd); err != nil {
		c.Logger.Fatalf("Error initializing a config option: %s", err.Error())
	}
	return cmd
}

func (c *serveCmd) Run(cfg serve.Configs) {
	err := serve.Serve(cfg)
	if err != nil {
		c.Logger.Fatalf("Error running Serve: %s", err.Error())
	}
}
