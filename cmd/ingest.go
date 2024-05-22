package cmd

import (
	"go/types"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/ingest"
)

type ingestCmd struct{}

func (c *ingestCmd) Command() *cobra.Command {
	cfg := ingest.Configs{}
	cfgOpts := config.ConfigOptions{
		{
			Name:        "database-url",
			Usage:       "Database connection URL.",
			OptType:     types.String,
			ConfigKey:   &cfg.DatabaseURL,
			FlagDefault: "postgres://postgres@localhost:5432/wallet-backend?sslmode=disable",
			Required:    true,
		},
		{
			Name:        "network-passphrase",
			Usage:       "Stellar Network Passphrase to connect.",
			OptType:     types.String,
			ConfigKey:   &cfg.NetworkPassphrase,
			FlagDefault: network.TestNetworkPassphrase,
			Required:    true,
		},
		{
			Name:        "captive-core-bin-path",
			Usage:       "Path to Captive Core's binary file.",
			OptType:     types.String,
			ConfigKey:   &cfg.CaptiveCoreBinPath,
			FlagDefault: "/usr/local/bin/stellar-core",
			Required:    true,
		},
		{
			Name:        "captive-core-config-dir",
			Usage:       "Path to Captive Core's configuration files directory.",
			OptType:     types.String,
			ConfigKey:   &cfg.CaptiveCoreConfigDir,
			FlagDefault: "./internal/ingest/config",
			Required:    true,
		},
		{
			Name:        "ledger-cursor-name",
			Usage:       "Name of last synced ledger cursor. Attention: there should never be more than one container running with a same cursor name.",
			OptType:     types.String,
			ConfigKey:   &cfg.LedgerCursorName,
			FlagDefault: "last_synced_ledger",
			Required:    false,
		},
		{
			Name:        "start",
			Usage:       "Ledger number from which ingestion should start. When not present, ingestion will resume from last synced ledger.",
			OptType:     types.Int,
			ConfigKey:   &cfg.StartLedger,
			FlagDefault: 0,
			Required:    false,
		},
		{
			Name:        "end",
			Usage:       "Ledger number up to which ingestion should run. When not present, ingestion run indefinitely (live ingestion requires it to be empty).",
			OptType:     types.Int,
			ConfigKey:   &cfg.EndLedger,
			FlagDefault: 0,
			Required:    false,
		},
	}

	cmd := &cobra.Command{
		Use:   "ingest",
		Short: "Run Ingestion service",
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

func (c *ingestCmd) Run(cfg ingest.Configs) {
	err := ingest.Ingest(cfg)
	if err != nil {
		log.Fatalf("Error running Ingest: %s", err.Error())
	}
}
