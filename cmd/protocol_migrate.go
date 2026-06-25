package cmd

import (
	"context"
	"fmt"
	"go/types"
	"io"
	"net/http"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/ingest"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	_ "github.com/stellar/wallet-backend/internal/services/sep41" // registers SEP-41 validator + processor via init()
)

type protocolMigrateCmd struct{}

func (c *protocolMigrateCmd) Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "protocol-migrate",
		Short: "Data migration commands for protocol state",
		Long:  "Parent command for protocol data migrations. Use subcommands to run specific migration tasks.",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cmd.Help(); err != nil {
				log.Fatalf("Error calling help command: %s", err.Error())
			}
		},
	}

	cmd.AddCommand(c.historyCommand())
	cmd.AddCommand(c.currentStateCommand())

	return cmd
}

// migrationCommandOpts captures the shared flags for migration subcommands.
type migrationCommandOpts struct {
	databaseURL       string
	rpcURL            string
	networkPassphrase string
	protocolIDs       []string
	logLevel          logrus.Level
	ledgerBackendType string
	datastore         ingest.DatastoreConfig
	getLedgersLimit   int
	windowSize        uint32
	metricsPort       int
}

// buildMigrationCommand creates a cobra.Command with shared migration flags and validation.
// addFlags adds strategy-specific flags. extraValidate runs strategy-specific validation.
// runE receives the shared opts and executes the strategy-specific logic.
func buildMigrationCommand(
	use, short, long string,
	addFlags func(cmd *cobra.Command, opts *migrationCommandOpts),
	extraValidate func() error,
	runE func(opts *migrationCommandOpts) error,
) *cobra.Command {
	var opts migrationCommandOpts

	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&opts.databaseURL),
		utils.NetworkPassphraseOption(&opts.networkPassphrase),
		utils.LogLevelOption(&opts.logLevel),
		// RPC URL is only required when --ledger-backend-type=rpc; validated in PersistentPreRunE.
		{
			Name:        "rpc-url",
			Usage:       "The URL of the RPC Server. Required when --ledger-backend-type=rpc.",
			OptType:     types.String,
			ConfigKey:   &opts.rpcURL,
			FlagDefault: "",
			Required:    false,
		},
		{
			Name:        "ledger-backend-type",
			Usage:       "Type of ledger backend to use for fetching historical ledgers. Options: 'rpc' or 'datastore' (default). Datastore is recommended for migrations because it can reach ledgers outside the RPC retention window.",
			OptType:     types.String,
			ConfigKey:   &opts.ledgerBackendType,
			FlagDefault: string(ingest.LedgerBackendTypeDatastore),
			Required:    false,
		},
		{
			Name:        "get-ledgers-limit",
			Usage:       "Per-request ledger buffer size for the RPC backend. Ignored for datastore.",
			OptType:     types.Int,
			ConfigKey:   &opts.getLedgersLimit,
			FlagDefault: 10,
			Required:    false,
		},
	}
	cfgOpts = append(cfgOpts, utils.DatastoreOptions(&opts.datastore)...)

	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Long:  long,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}
			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}

			log.DefaultLogger.SetLevel(opts.logLevel)

			if len(opts.protocolIDs) == 0 {
				return fmt.Errorf("at least one --protocol-id is required")
			}

			// Per-backend required-field validation.
			switch opts.ledgerBackendType {
			case string(ingest.LedgerBackendTypeRPC):
				if opts.rpcURL == "" {
					return fmt.Errorf("--rpc-url is required when --ledger-backend-type=rpc")
				}
			case string(ingest.LedgerBackendTypeDatastore):
				if opts.datastore.BucketPath == "" {
					return fmt.Errorf("--datastore-bucket-path is required when --ledger-backend-type=datastore")
				}
			default:
				return fmt.Errorf("invalid --ledger-backend-type %q, must be 'rpc' or 'datastore'", opts.ledgerBackendType)
			}

			if extraValidate != nil {
				return extraValidate()
			}
			return nil
		},
		RunE: func(_ *cobra.Command, _ []string) error {
			return runE(&opts)
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	cmd.Flags().StringSliceVar(&opts.protocolIDs, "protocol-id", nil, "Protocol ID(s) to migrate (required, repeatable)")
	cmd.Flags().Uint32Var(&opts.windowSize, "window-size", 100, "Ledgers coalesced into one commit (0 or 1 = commit every ledger)")
	cmd.Flags().IntVar(&opts.metricsPort, "metrics-port", 0, "Port to expose Prometheus /metrics on. 0 disables (default).")

	if addFlags != nil {
		addFlags(cmd, &opts)
	}

	return cmd
}

// runMigration handles the shared setup (processors, DB, models, ledger backend) and
// delegates to createAndRun for strategy-specific service creation and execution.
func runMigration(
	label string,
	opts *migrationCommandOpts,
	createAndRun func(
		ctx context.Context,
		dbPool *pgxpool.Pool,
		ledgerBackend ledgerbackend.LedgerBackend,
		models *data.Models,
		processors []services.ProtocolProcessor,
		migrationMetrics *metrics.MigrationMetrics,
		tipProvider func() (uint32, error),
	) error,
) error {
	ctx := context.Background()

	log.Ctx(ctx).Infof("Starting protocol-migrate %s for protocols: %v", label, opts.protocolIDs)

	// Open DB connection
	dbPool, err := db.OpenDBConnectionPool(ctx, opts.databaseURL)
	if err != nil {
		return fmt.Errorf("opening database connection: %w", err)
	}
	defer dbPool.Close()

	// Create models so the per-protocol processor factories (registered via
	// services.RegisterProcessor) can pull what they need from ProtocolDeps.
	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(dbPool, m.DB)
	if err != nil {
		return fmt.Errorf("creating models: %w", err)
	}

	if opts.metricsPort > 0 {
		metricsServer := startMigrationMetricsServer(opts.metricsPort, m.Registry())
		defer func() {
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelShutdown()
			if shErr := metricsServer.Shutdown(shutdownCtx); shErr != nil {
				log.Ctx(ctx).Warnf("error shutting down metrics server: %v", shErr)
			}
		}()
	}

	// ContractMetadataService is wired in only if an RPC URL is available; the
	// protocol-migrate CLI does not strictly require RPC (datastore backend
	// runs without one), but per-protocol contract metadata enrichment on
	// first-ingest does. A nil ContractMetadataService is tolerated by
	// processors — they fall back to defaults.
	var tipProvider func() (uint32, error)
	var metadataService services.ContractMetadataService
	if opts.rpcURL != "" {
		rpcService, rpcErr := services.NewRPCService(
			opts.rpcURL,
			opts.networkPassphrase,
			// Keep-alives disabled (see keepAlivesDisabledHTTPClient): a fresh connection per request
			// sidesteps stale-connection EOFs behind intermediaries that don't support HTTP connection
			// reuse; negligible at this command's RPC volume.
			keepAlivesDisabledHTTPClient(30*time.Second),
			m.RPC,
		)
		if rpcErr != nil {
			return fmt.Errorf("instantiating rpc service for metadata fetcher: %w", rpcErr)
		}
		tipProvider = func() (uint32, error) {
			h, healthErr := rpcService.GetHealth()
			if healthErr != nil {
				return 0, fmt.Errorf("getting rpc health for migration chain tip: %w", healthErr)
			}
			return h.LatestLedger, nil
		}
		metadataPool := pond.NewPool(0)
		defer metadataPool.StopAndWait()
		cms, cmsErr := services.NewContractMetadataService(rpcService, models.Contract, metadataPool)
		if cmsErr != nil {
			return fmt.Errorf("instantiating contract metadata service: %w", cmsErr)
		}
		metadataService = cms
	}

	deps := services.ProtocolDeps{
		NetworkPassphrase:       opts.networkPassphrase,
		Models:                  models,
		ContractMetadataService: metadataService,
		MetricsService:          m,
	}
	processors, err := services.BuildProcessors(deps, opts.protocolIDs)
	if err != nil {
		return fmt.Errorf("building protocol processors: %w", err)
	}

	// Build a ledger backend using the same selector the ingest service uses,
	// so protocol-migrate inherits the datastore path (recommended for
	// backfills — unbounded history, unlike RPC retention windows).
	ledgerBackend, err := ingest.NewLedgerBackend(ctx, ingest.Configs{
		LedgerBackendType: ingest.LedgerBackendType(opts.ledgerBackendType),
		Datastore:         opts.datastore,
		NetworkPassphrase: opts.networkPassphrase,
		RPCURL:            opts.rpcURL,
		GetLedgersLimit:   opts.getLedgersLimit,
	})
	if err != nil {
		return fmt.Errorf("creating ledger backend: %w", err)
	}
	defer func() {
		if closer, ok := ledgerBackend.(io.Closer); ok {
			if closeErr := closer.Close(); closeErr != nil {
				log.Ctx(ctx).Errorf("error closing ledger backend: %v", closeErr)
			}
		}
	}()

	return createAndRun(ctx, dbPool, ledgerBackend, models, processors, m.Migration, tipProvider)
}

func (c *protocolMigrateCmd) historyCommand() *cobra.Command {
	var oldestLedgerCursorName string

	return buildMigrationCommand(
		"history",
		"Backfill protocol history state from oldest to latest ingested ledger",
		"Processes historical ledgers from oldest_ingest_ledger to the tip, producing protocol state changes and converging with live ingestion via CAS-gated cursors.",
		func(cmd *cobra.Command, opts *migrationCommandOpts) {
			cmd.Flags().StringVar(&oldestLedgerCursorName, "oldest-ledger-cursor-name", data.OldestLedgerCursorName, "Name of the oldest ledger cursor in the ingest store. Must match the value used by the ingest service.")
		},
		nil,
		func(opts *migrationCommandOpts) error {
			return runMigration("history", opts, func(ctx context.Context, dbPool *pgxpool.Pool, ledgerBackend ledgerbackend.LedgerBackend, models *data.Models, processors []services.ProtocolProcessor, migrationMetrics *metrics.MigrationMetrics, tipProvider func() (uint32, error)) error {
				service, err := services.NewProtocolMigrateHistoryService(services.ProtocolMigrateHistoryConfig{
					DB:                     dbPool,
					LedgerBackend:          ledgerBackend,
					ProtocolsModel:         models.Protocols,
					ProtocolContractsModel: models.ProtocolContracts,
					IngestStore:            models.IngestStore,
					NetworkPassphrase:      opts.networkPassphrase,
					Processors:             processors,
					OldestLedgerCursorName: oldestLedgerCursorName,
					WindowSize:             opts.windowSize,
					Metrics:                migrationMetrics,
					TipProvider:            tipProvider,
				})
				if err != nil {
					return fmt.Errorf("creating protocol migrate history service: %w", err)
				}
				if err := service.Run(ctx, opts.protocolIDs); err != nil {
					return fmt.Errorf("running protocol migrate history: %w", err)
				}
				return nil
			})
		},
	)
}

func (c *protocolMigrateCmd) currentStateCommand() *cobra.Command {
	var startLedger uint32

	return buildMigrationCommand(
		"current-state",
		"Build protocol current state from a start ledger forward",
		"Processes ledgers from --start-ledger to the tip, building protocol current state and converging with live ingestion via CAS-gated cursors.",
		func(cmd *cobra.Command, opts *migrationCommandOpts) {
			cmd.Flags().Uint32Var(&startLedger, "start-ledger", 0, "Ledger sequence to begin current-state migration from (required)")
		},
		func() error {
			if startLedger == 0 {
				return fmt.Errorf("--start-ledger is required and must be > 0")
			}
			return nil
		},
		func(opts *migrationCommandOpts) error {
			return runMigration("current-state", opts, func(ctx context.Context, dbPool *pgxpool.Pool, ledgerBackend ledgerbackend.LedgerBackend, models *data.Models, processors []services.ProtocolProcessor, migrationMetrics *metrics.MigrationMetrics, tipProvider func() (uint32, error)) error {
				service, err := services.NewProtocolMigrateCurrentStateService(services.ProtocolMigrateCurrentStateConfig{
					DB:                     dbPool,
					LedgerBackend:          ledgerBackend,
					ProtocolsModel:         models.Protocols,
					ProtocolContractsModel: models.ProtocolContracts,
					IngestStore:            models.IngestStore,
					NetworkPassphrase:      opts.networkPassphrase,
					Processors:             processors,
					StartLedger:            startLedger,
					WindowSize:             opts.windowSize,
					Metrics:                migrationMetrics,
					TipProvider:            tipProvider,
				})
				if err != nil {
					return fmt.Errorf("creating protocol migrate current-state service: %w", err)
				}
				if err := service.Run(ctx, opts.protocolIDs); err != nil {
					return fmt.Errorf("running protocol migrate current-state: %w", err)
				}
				return nil
			})
		},
	)
}

// migrationMetricsHandler serves the Prometheus registry at /metrics.
func migrationMetricsHandler(reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}

// startMigrationMetricsServer starts a goroutine serving /metrics on the given
// port for the life of the migration run. The returned server is shut down by
// the caller when Run completes.
func startMigrationMetricsServer(port int, reg *prometheus.Registry) *http.Server {
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           migrationMetricsHandler(reg),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Infof("Starting protocol-migrate metrics server on port %d at /metrics", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("metrics server on %s stopped: %v", server.Addr, err)
		}
	}()
	return server
}
