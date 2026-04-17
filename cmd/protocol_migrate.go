package cmd

import (
	"context"
	"fmt"
	"go/types"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
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
	"github.com/stellar/wallet-backend/internal/services/sep41"
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
	databaseURL         string
	rpcURL              string
	networkPassphrase   string
	protocolIDs         []string
	logLevel            string
	ledgerBackendType   string
	datastoreConfigPath string
	getLedgersLimit     int
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
			Name:        "datastore-config-path",
			Usage:       "Path to TOML config file for datastore backend. Required when --ledger-backend-type=datastore.",
			OptType:     types.String,
			ConfigKey:   &opts.datastoreConfigPath,
			FlagDefault: "config/datastore-pubnet.toml",
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

			if opts.logLevel != "" {
				ll, err := logrus.ParseLevel(opts.logLevel)
				if err != nil {
					return fmt.Errorf("invalid log level %q: %w", opts.logLevel, err)
				}
				log.DefaultLogger.SetLevel(ll)
			}

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
				if opts.datastoreConfigPath == "" {
					return fmt.Errorf("--datastore-config-path is required when --ledger-backend-type=datastore")
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
	cmd.Flags().StringVar(&opts.logLevel, "log-level", "", `Log level: "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "PANIC"`)
	var deprecatedLatestLedgerCursorName string
	cmd.Flags().StringVar(&deprecatedLatestLedgerCursorName, "latest-ledger-cursor-name", "", "DEPRECATED: ignored. The latest ledger cursor name is now hard-coded.")
	if err := cmd.Flags().MarkDeprecated("latest-ledger-cursor-name", "ignored; the cursor name is now hard-coded"); err != nil {
		log.Fatalf("marking latest-ledger-cursor-name deprecated: %s", err.Error())
	}

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

	// Create models first so processor factories (which capture dependencies via SetDependencies)
	// have the data-layer objects they need when invoked.
	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(dbPool, m.DB)
	if err != nil {
		return fmt.Errorf("creating models: %w", err)
	}

	sep41.SetDependencies(sep41.Dependencies{
		NetworkPassphrase: opts.networkPassphrase,
		Balances:          models.SEP41.Balances,
		Allowances:        models.SEP41.Allowances,
		ContractTokens:    models.Contract,
	})

	// Build processors from protocol IDs using the dynamic registry.
	var processors []services.ProtocolProcessor
	for _, pid := range opts.protocolIDs {
		factory, ok := services.GetProcessor(pid)
		if !ok {
			return fmt.Errorf("unknown protocol ID %q — no processor registered", pid)
		}
		p := factory()
		if p == nil {
			return fmt.Errorf("processor factory for protocol %q returned nil", pid)
		}
		processors = append(processors, p)
	}

	// Build a ledger backend using the same selector the ingest service uses,
	// so protocol-migrate inherits the datastore path (recommended for
	// backfills — unbounded history, unlike RPC retention windows).
	ledgerBackend, err := ingest.NewLedgerBackend(ctx, ingest.Configs{
		LedgerBackendType:   ingest.LedgerBackendType(opts.ledgerBackendType),
		DatastoreConfigPath: opts.datastoreConfigPath,
		NetworkPassphrase:   opts.networkPassphrase,
		RPCURL:              opts.rpcURL,
		GetLedgersLimit:     opts.getLedgersLimit,
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

	return createAndRun(ctx, dbPool, ledgerBackend, models, processors)
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
			return runMigration("history", opts, func(ctx context.Context, dbPool *pgxpool.Pool, ledgerBackend ledgerbackend.LedgerBackend, models *data.Models, processors []services.ProtocolProcessor) error {
				service, err := services.NewProtocolMigrateHistoryService(services.ProtocolMigrateHistoryConfig{
					DB:                     dbPool,
					LedgerBackend:          ledgerBackend,
					ProtocolsModel:         models.Protocols,
					ProtocolContractsModel: models.ProtocolContracts,
					IngestStore:            models.IngestStore,
					NetworkPassphrase:      opts.networkPassphrase,
					Processors:             processors,
					OldestLedgerCursorName: oldestLedgerCursorName,
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
			return runMigration("current-state", opts, func(ctx context.Context, dbPool *pgxpool.Pool, ledgerBackend ledgerbackend.LedgerBackend, models *data.Models, processors []services.ProtocolProcessor) error {
				service, err := services.NewProtocolMigrateCurrentStateService(services.ProtocolMigrateCurrentStateConfig{
					DB:                     dbPool,
					LedgerBackend:          ledgerBackend,
					ProtocolsModel:         models.Protocols,
					ProtocolContractsModel: models.ProtocolContracts,
					IngestStore:            models.IngestStore,
					NetworkPassphrase:      opts.networkPassphrase,
					Processors:             processors,
					StartLedger:            startLedger,
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
