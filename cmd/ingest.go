package cmd

import (
	"fmt"
	"go/types"
	"math"

	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/ingest"
	"github.com/stellar/wallet-backend/internal/services"
)

type ingestCmd struct{}

func (c *ingestCmd) Command() *cobra.Command {
	cfg := ingest.Configs{}
	var ledgerBackendType string
	startLedgerOpt := utils.StartLedgerOption(&cfg.StartLedger)
	endLedgerOpt := utils.EndLedgerOption(&cfg.EndLedger)
	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&cfg.DatabaseURL),
		utils.LogLevelOption(&cfg.LogLevel),
		utils.RPCURLOption(&cfg.RPCURL),
		startLedgerOpt,
		endLedgerOpt,
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
			Name:        "backfill-flush-size",
			Usage:       "Ledgers written per database flush during backfill. Each flush is one transaction per bulk table. Memory grows with this value plus the datastore prefetch and the worker count.",
			OptType:     types.Int,
			ConfigKey:   &cfg.BackfillFlushSize,
			FlagDefault: 100,
			Required:    false,
		},
		{
			Name:        "backfill-compress-concurrency",
			Usage:       "Chunks compressed at once during backfill, each on its own database connection. The run keeps at most ceil(value/5)+1 windows uncompressed on disk.",
			OptType:     types.Int,
			ConfigKey:   &cfg.BackfillCompressConcurrency,
			FlagDefault: 5,
			Required:    false,
		},
		{
			Name:        "live-persist-max-batch-size",
			Usage:       "Maximum consecutive ledgers coalesced into one persist commit when live ingestion falls behind. The default 1 persists every ledger in its own commit — right for networks whose close time comfortably exceeds the persist time; raise it on high-TPL/short-block deployments to amortize inserts across a backlog at the cost of coarser crash recovery and visibility latency under load. The pipeline holds up to 2× this value + 1 ledger buffers at peak — check memory headroom before raising it under GOMEMLIMIT.",
			OptType:     types.Int,
			ConfigKey:   &cfg.LivePersistMaxBatchSize,
			FlagDefault: 1,
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
			Name:        "tx-hash-chunk-interval",
			Usage:       "TimescaleDB chunk interval for the transaction_hashes lookup table. Only affects future chunks. Keep a few million transactions per chunk: the table is segmented into 4096 buckets and a chunk with fewer rows compresses into near-empty batches (measured 8 bytes per row at 2M rows, 36 at 130k, 250 at 20k). Each hash lookup also checks every chunk, so longer is better, until the open chunk's (hash_bucket, hash_prefix) index (about 27 bytes per transaction) no longer fits in shared_buffers. Uses PostgreSQL INTERVAL syntax.",
			OptType:     types.String,
			ConfigKey:   &cfg.TransactionHashesChunkInterval,
			FlagDefault: "7 days",
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
			Usage:       "Maximum chunks compressed per compression job run (maxchunks_to_compress). 0 leaves the job's current setting unchanged.",
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
				// silently ignored, so reject any explicitly supplied value, including 0.
				if config.IsExplicitlySet(startLedgerOpt) || config.IsExplicitlySet(endLedgerOpt) {
					return fmt.Errorf("--start-ledger applies to --ingestion-mode=backfill only and --end-ledger is not used by ingest; live mode resumes from the stored cursor")
				}
			case services.IngestionModeBackfill:
				if cfg.StartLedger <= 0 || cfg.StartLedger >= math.MaxUint32 {
					return fmt.Errorf("--ingestion-mode=backfill needs --start-ledger between 1 and %d (got %d)", uint32(math.MaxUint32)-1, cfg.StartLedger)
				}
				if config.IsExplicitlySet(endLedgerOpt) {
					return fmt.Errorf("--end-ledger does not apply to backfill: the run fills every ledger from --start-ledger up to the live ingester's history")
				}
				if cfg.BackfillFlushSize <= 0 {
					return fmt.Errorf("--backfill-flush-size must be positive (got %d)", cfg.BackfillFlushSize)
				}
				if cfg.BackfillCompressConcurrency <= 0 {
					return fmt.Errorf("--backfill-compress-concurrency must be positive (got %d)", cfg.BackfillCompressConcurrency)
				}
				if cfg.BackfillWorkers < 0 {
					return fmt.Errorf("--backfill-workers must be 0 (one per CPU) or positive (got %d)", cfg.BackfillWorkers)
				}
			default:
				return fmt.Errorf("invalid ingestion-mode '%s', must be 'live' or 'backfill'", cfg.IngestionMode)
			}
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
