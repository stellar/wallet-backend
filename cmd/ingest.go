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
	var ledgerBackendType string
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
		utils.RedisHostOption(&cfg.RedisHost),
		utils.RedisPortOption(&cfg.RedisPort),
		{
			Name:        "ledger-cursor-name",
			Usage:       "Name of last synced ledger cursor, used to keep track of the last ledger ingested by the service. When starting up, ingestion will resume from the ledger number stored in this record. It should be an unique name per container as different containers would overwrite the cursor value of its peers when using the same cursor name.",
			OptType:     types.String,
			ConfigKey:   &cfg.LedgerCursorName,
			FlagDefault: "live_ingest_cursor",
			Required:    true,
		},
		{
			Name:        "account-tokens-cursor-name",
			Usage:       "Name of last synced account tokens ledger cursor, used to keep track of the last ledger ingested by the service.",
			OptType:     types.String,
			ConfigKey:   &cfg.AccountTokensCursorName,
			FlagDefault: "live_account_tokens_ingest_cursor",
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
			Usage:       "Checkpoint frequency for history archive (number of ledgers between checkpoints). Use 64 for production usage, 8 for integration tests with accelerated time.",
			OptType:     types.Int,
			ConfigKey:   &cfg.CheckpointFrequency,
			FlagDefault: 64,
			Required:    false,
		},
		{
			Name:        "ledger-backend-type",
			Usage:       "Type of ledger backend to use for fetching ledgers. Options: 'rpc' (default) or 'datastore'",
			OptType:     types.String,
			ConfigKey:   &ledgerBackendType,
			FlagDefault: string(ingest.LedgerBackendTypeDatastore),
			Required:    false,
		},
		{
			Name:        "datastore-config-path",
			Usage:       "Path to TOML config file for datastore backend. Required when ledger-backend-type is 'datastore'",
			OptType:     types.String,
			ConfigKey:   &cfg.DatastoreConfigPath,
			FlagDefault: "config/datastore-pubnet.toml",
			Required:    false,
		},
		{
			Name:        "skip-tx-meta",
			Usage:       "Skip storing transaction metadata (meta_xdr) to reduce storage space and improve insertion performance.",
			OptType:     types.Bool,
			ConfigKey:   &cfg.SkipTxMeta,
			FlagDefault: true,
			Required:    false,
		},
		{
			Name:        "skip-tx-envelope",
			Usage:       "Skip storing transaction envelope (envelope_xdr) to reduce storage space and improve insertion performance.",
			OptType:     types.Bool,
			ConfigKey:   &cfg.SkipTxEnvelope,
			FlagDefault: true,
			Required:    false,
		},
		{
			Name:        "enable-participant-filtering",
			Usage:       "When enabled, only store transactions, operations, and state changes for pre-registered accounts. When disabled (default), store all data.",
			OptType:     types.Bool,
			ConfigKey:   &cfg.EnableParticipantFiltering,
			FlagDefault: false,
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

			// Convert ledger backend type string to typed value
			switch ledgerBackendType {
			case string(ingest.LedgerBackendTypeRPC):
				cfg.LedgerBackendType = ingest.LedgerBackendTypeRPC
			case string(ingest.LedgerBackendTypeDatastore):
				cfg.LedgerBackendType = ingest.LedgerBackendTypeDatastore
			default:
				return fmt.Errorf("invalid ledger-backend-type '%s', must be 'rpc' or 'datastore'", ledgerBackendType)
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
