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
		utils.RPCURLOption(&cfg.RPCURL),
		utils.StartLedgerOption(&cfg.StartLedger),
		utils.EndLedgerOption(&cfg.EndLedger),
		utils.NetworkPassphraseOption(&cfg.NetworkPassphrase),
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
			Name:        "start",
			Usage:       "Ledger number from which ingestion should start. When not present, ingestion will resume from last synced ledger.",
			OptType:     types.Int,
			ConfigKey:   &cfg.StartLedger,
			FlagDefault: 0,
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
