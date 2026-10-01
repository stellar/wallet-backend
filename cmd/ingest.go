package cmd

import (
	"fmt"
	"go/types"

	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/apptracker/sentry"
	"github.com/stellar/wallet-backend/internal/ingest"
	"github.com/stellar/wallet-backend/internal/services"
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
		utils.RPCURLOption(&cfg.RPCURL),
		utils.StartLedgerOption(&cfg.StartLedger),
		utils.EndLedgerOption(&cfg.EndLedger),
		utils.NetworkPassphraseOption(&cfg.NetworkPassphrase),
		utils.IngestServerPortOption(&cfg.ServerPort),
		utils.AdminPortOption(&cfg.AdminPort),
		utils.GetLedgersLimitOption(&cfg.GetLedgersLimit),
		{
			Name:        "ingestion-mode",
			Usage:       "What mode to run ingestion in - live or backfill",
			OptType:     types.String,
			ConfigKey:   &cfg.IngestionMode,
			FlagDefault: services.IngestionModeLive,
			Required:    true,
		},
		{
			Name:        "backfill-workers",
			Usage:       "Maximum concurrent workers for backfill processing. Defaults to number of CPUs. Lower values reduce RAM usage at cost of throughput.",
			OptType:     types.Int,
			ConfigKey:   &cfg.BackfillWorkers,
			FlagDefault: 0,
			Required:    false,
		},
		{
			Name:        "backfill-batch-size",
			Usage:       "Number of ledgers per batch during backfill. Defaults to 250. Lower values reduce RAM usage at cost of more DB transactions.",
			OptType:     types.Int,
			ConfigKey:   &cfg.BackfillBatchSize,
			FlagDefault: 250,
			Required:    false,
		},
		{
			Name:        "backfill-db-insert-batch-size",
			Usage:       "Number of ledgers to process before flushing buffer to DB during backfill. Defaults to 100. Lower values reduce RAM usage at cost of more DB transactions.",
			OptType:     types.Int,
			ConfigKey:   &cfg.BackfillDBInsertBatchSize,
			FlagDefault: 100,
			Required:    false,
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
			Usage:       "Where ledgers are read from. 'rpc' (default) streams them from the RPC at rpc-url; 'datastore' reads them from the S3 data lake at datastore-bucket-path.",
			OptType:     types.String,
			ConfigKey:   &ledgerBackendType,
			FlagDefault: string(ingest.LedgerBackendTypeRPC),
			Required:    false,
		},
		{
			Name:        "chunk-interval",
			Usage:       "TimescaleDB chunk time interval for hypertables. Only affects future chunks. Uses PostgreSQL INTERVAL syntax.",
			OptType:     types.String,
			ConfigKey:   &cfg.ChunkInterval,
			FlagDefault: "1 day",
			Required:    false,
		},
		{
			Name:        "retention-period",
			Usage:       "TimescaleDB data retention period. Chunks older than this are automatically dropped. Empty disables retention. Uses PostgreSQL INTERVAL syntax.",
			OptType:     types.String,
			ConfigKey:   &cfg.RetentionPeriod,
			FlagDefault: "",
			Required:    false,
		},
		{
			Name:        "compression-schedule-interval",
			Usage:       "How frequently the TimescaleDB compression policy job checks for chunks to compress. Does not change which chunks are eligible (that's controlled by compress_after). Empty skips configuration. Uses PostgreSQL INTERVAL syntax.",
			OptType:     types.String,
			ConfigKey:   &cfg.CompressionScheduleInterval,
			FlagDefault: "",
			Required:    false,
		},
		{
			Name:        "compression-compress-after",
			Usage:       "How long after a chunk is closed before it becomes eligible for compression. Lower values reduce the number of uncompressed chunks. Empty skips configuration. Uses PostgreSQL INTERVAL syntax.",
			OptType:     types.String,
			ConfigKey:   &cfg.CompressAfter,
			FlagDefault: "",
			Required:    false,
		},
		{
			Name:        "compression-max-chunks",
			Usage:       "Maximum chunks compressed per job run (maxchunks_to_compress). 0 means unlimited (TimescaleDB default). Set to a small value (e.g. 10) during backfill to prevent job run overlap.",
			OptType:     types.Int,
			ConfigKey:   &cfg.MaxChunksToCompress,
			FlagDefault: 0,
			Required:    false,
		},
	}

	cfgOpts = append(cfgOpts, utils.DBPoolOptions(&cfg.DBMaxConns, &cfg.DBMinConns, &cfg.DBMaxConnLifetime, &cfg.DBMaxConnIdleTime)...)
	cfgOpts = append(cfgOpts, utils.DatastoreOptions(&cfg.Datastore)...)

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

			switch ledgerBackendType {
			case string(ingest.LedgerBackendTypeRPC):
				cfg.LedgerBackendType = ingest.LedgerBackendTypeRPC
			case string(ingest.LedgerBackendTypeDatastore):
				cfg.LedgerBackendType = ingest.LedgerBackendTypeDatastore
				if cfg.Datastore.BucketPath == "" {
					return fmt.Errorf("--datastore-bucket-path (DATASTORE_BUCKET_PATH) is required when --ledger-backend-type=datastore")
				}
			default:
				return fmt.Errorf("invalid ledger-backend-type '%s', must be 'rpc' or 'datastore'", ledgerBackendType)
			}

			switch cfg.IngestionMode {
			case services.IngestionModeLive:
				// Live mode resumes from the stored cursor; a start or end ledger would be
				// silently ignored, so reject it instead.
				if cfg.StartLedger != 0 || cfg.EndLedger != 0 {
					return fmt.Errorf("--start-ledger and --end-ledger apply to --ingestion-mode=backfill only; live mode resumes from the stored cursor")
				}
			case services.IngestionModeBackfill:
				if cfg.StartLedger <= 0 || cfg.EndLedger < cfg.StartLedger {
					return fmt.Errorf("--ingestion-mode=backfill needs --start-ledger > 0 and --end-ledger >= --start-ledger (got %d..%d)", cfg.StartLedger, cfg.EndLedger)
				}
			default:
				return fmt.Errorf("invalid ingestion-mode '%s', must be 'live' or 'backfill'", cfg.IngestionMode)
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
