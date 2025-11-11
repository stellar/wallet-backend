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
	"github.com/stellar/wallet-backend/internal/ingest"
)

type ingestCmd struct{}

func (c *ingestCmd) Command() *cobra.Command {
	cfg := ingest.Configs{}
	var sentryDSN string
	var stellarEnvironment string
	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&cfg.DatabaseURL),
		utils.LogLevelOption(&cfg.LogLevel),
		utils.SentryDSNOption(&sentryDSN),
		utils.StellarEnvironmentOption(&stellarEnvironment),
		utils.NetworkOption(&cfg.Network),
		utils.RPCURLOption(&cfg.RPCURL),
		utils.StartLedgerOption(&cfg.StartLedger),
		utils.EndLedgerOption(&cfg.EndLedger),
		utils.NetworkPassphraseOption(&cfg.NetworkPassphrase),
		utils.IngestServerPortOption(&cfg.ServerPort),
		utils.AdminPortOption(&cfg.AdminPort),
		utils.GetLedgersLimitOption(&cfg.GetLedgersLimit),
		{
			Name:        "ledger-cursor-name",
			Usage:       "Name of last synced ledger cursor, used to keep track of the last ledger ingested by the service. When starting up, ingestion will resume from the ledger number stored in this record. It should be an unique name per container as different containers would overwrite the cursor value of its peers when using the same cursor name.",
			OptType:     types.String,
			ConfigKey:   &cfg.LedgerCursorName,
			FlagDefault: "live_ingest_cursor",
			Required:    true,
		},
		{
			Name:        "trustlines-cursor-name",
			Usage:       "Name of last synced trustlines ledger cursor, used to keep track of the last trustlines ledger ingested by the service.",
			OptType:     types.String,
			ConfigKey:   &cfg.TrustlinesCursorName,
			FlagDefault: "live_trustlines_ingest_cursor",
			Required:    true,
		},
		{
			Name:        "archive-url",
			Usage:       "Archive URL for history archives",
			OptType:     types.String,
			ConfigKey:   &cfg.ArchiveURL,
			FlagDefault: "https://history.stellar.org/prd/core-testnet/core_testnet_001/",
			Required:    true,
		},
		{
			Name:        "checkpoint-frequency",
			Usage:       "Checkpoint frequency for history archive (number of ledgers between checkpoints). Use 64 for production networks, 8 for integration tests with accelerated time.",
			OptType:     types.Int,
			ConfigKey:   &cfg.CheckpointFrequency,
			FlagDefault: 64,
			Required:    false,
		},
		{
			Name:        "redis-host",
			Usage:       "Redis host for caching",
			OptType:     types.String,
			ConfigKey:   &cfg.RedisHost,
			FlagDefault: "localhost",
			Required:    false,
		},
		{
			Name:        "redis-port",
			Usage:       "Redis port for caching",
			OptType:     types.Int,
			ConfigKey:   &cfg.RedisPort,
			FlagDefault: 6379,
			Required:    false,
		},
		{
			Name:        "redis-password",
			Usage:       "Redis password for caching",
			OptType:     types.String,
			ConfigKey:   &cfg.RedisPassword,
			FlagDefault: "",
			Required:    false,
		},
	}

	cmd := &cobra.Command{
		Use:   "ingest",
		Short: "Run Ingestion service",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}
			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}
			appTracker, err := sentry.NewSentryTracker(sentryDSN, stellarEnvironment, 5)
			if err != nil {
				return fmt.Errorf("initializing app tracker: %w", err)
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

func (c *ingestCmd) Run(cfg ingest.Configs) error {
	err := ingest.Ingest(cfg)
	if err != nil {
		return fmt.Errorf("running ingest: %w", err)
	}
	return nil
}
