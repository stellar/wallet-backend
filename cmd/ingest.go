package cmd

import (
	"fmt"
	"go/types"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/ingest"
)

type ingestCmd struct{}

func (c *ingestCmd) Command() *cobra.Command {
	cfg := ingest.Configs{}
	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&cfg.DatabaseURL),
		utils.LogLevelOption(&cfg.LogLevel),
		utils.NetworkPassphraseOption(&cfg.NetworkPassphrase),
		{
			Name:           "captive-core-bin-path",
			Usage:          "Path to Captive Core's binary file.",
			OptType:        types.String,
			CustomSetValue: utils.SetConfigOptionCaptiveCoreBinPath,
			ConfigKey:      &cfg.CaptiveCoreBinPath,
			FlagDefault:    "/usr/local/bin/stellar-core",
			Required:       true,
		},
		{
			Name:           "captive-core-config-dir",
			Usage:          "Path to Captive Core's configuration files directory.",
			OptType:        types.String,
			CustomSetValue: utils.SetConfigOptionCaptiveCoreConfigDir,
			ConfigKey:      &cfg.CaptiveCoreConfigDir,
			FlagDefault:    "./internal/ingest/config",
			Required:       true,
		},
		{
			Name:        "ledger-cursor-name",
			Usage:       "Name of last synced ledger cursor, used to keep track of the last ledger ingested by the service. When starting up, ingestion will resume from the ledger number stored in this record. It should be an unique name per container as different containers would overwrite the cursor value of its peers when using the same cursor name.",
			OptType:     types.String,
			ConfigKey:   &cfg.LedgerCursorName,
			FlagDefault: "live_ingest_cursor",
			Required:    true,
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
		Use:               "ingest",
		Short:             "Run Ingestion service",
		PersistentPreRunE: utils.DefaultPersistentPreRunE(cfgOpts),
		RunE: func(_ *cobra.Command, _ []string) error {
			return c.Run(cfg)
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	return cmd
}

func (c *ingestCmd) Run(cfg ingest.Configs) error {
	err := ingest.Ingest(cfg)
	if err != nil {
		return fmt.Errorf("running ingest: %w", err)
	}
	return nil
}
