package cmd

import (
	"context"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	internalutils "github.com/stellar/wallet-backend/internal/utils"
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

	return cmd
}

func (c *protocolMigrateCmd) historyCommand() *cobra.Command {
	var databaseURL string
	var rpcURL string
	var networkPassphrase string
	var protocolIDs []string
	var logLevel string
	var latestLedgerCursorName string
	var oldestLedgerCursorName string

	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&databaseURL),
		utils.RPCURLOption(&rpcURL),
		utils.NetworkPassphraseOption(&networkPassphrase),
	}

	cmd := &cobra.Command{
		Use:   "history",
		Short: "Backfill protocol history state from oldest to latest ingested ledger",
		Long:  "Processes historical ledgers from oldest_ingest_ledger to the tip, producing protocol state changes and converging with live ingestion via CAS-gated cursors.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}
			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}

			if logLevel != "" {
				ll, err := logrus.ParseLevel(logLevel)
				if err != nil {
					return fmt.Errorf("invalid log level %q: %w", logLevel, err)
				}
				log.DefaultLogger.SetLevel(ll)
			}

			if len(protocolIDs) == 0 {
				return fmt.Errorf("at least one --protocol-id is required")
			}
			return nil
		},
		RunE: func(_ *cobra.Command, _ []string) error {
			return c.RunHistory(databaseURL, rpcURL, networkPassphrase, protocolIDs, latestLedgerCursorName, oldestLedgerCursorName)
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	cmd.Flags().StringSliceVar(&protocolIDs, "protocol-id", nil, "Protocol ID(s) to migrate (required, repeatable)")
	cmd.Flags().StringVar(&logLevel, "log-level", "", `Log level: "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "PANIC"`)
	cmd.Flags().StringVar(&latestLedgerCursorName, "latest-ledger-cursor-name", data.LatestLedgerCursorName, "Name of the latest ledger cursor in the ingest store. Must match the value used by the ingest service.")
	cmd.Flags().StringVar(&oldestLedgerCursorName, "oldest-ledger-cursor-name", data.OldestLedgerCursorName, "Name of the oldest ledger cursor in the ingest store. Must match the value used by the ingest service.")

	return cmd
}

func (c *protocolMigrateCmd) RunHistory(databaseURL, rpcURL, networkPassphrase string, protocolIDs []string, latestLedgerCursorName, oldestLedgerCursorName string) error {
	ctx := context.Background()

	// Build processors from protocol IDs using the dynamic registry
	var processors []services.ProtocolProcessor
	for _, pid := range protocolIDs {
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

	// Open DB connection
	dbPool, err := db.OpenDBConnectionPool(databaseURL)
	if err != nil {
		return fmt.Errorf("opening database connection: %w", err)
	}
	defer internalutils.DeferredClose(ctx, dbPool, "closing dbPool in protocol migrate history")

	// Create models
	sqlxDB, err := dbPool.SqlxDB(ctx)
	if err != nil {
		return fmt.Errorf("getting sqlx DB: %w", err)
	}
	metricsService := metrics.NewMetricsService(sqlxDB)
	models, err := data.NewModels(dbPool, metricsService)
	if err != nil {
		return fmt.Errorf("creating models: %w", err)
	}

	// Create ledger backend for fetching historical ledgers
	ledgerBackend := ledgerbackend.NewRPCLedgerBackend(ledgerbackend.RPCLedgerBackendOptions{
		RPCServerURL: rpcURL,
		BufferSize:   10,
	})
	defer func() {
		if closeErr := ledgerBackend.Close(); closeErr != nil {
			log.Ctx(ctx).Errorf("error closing ledger backend: %v", closeErr)
		}
	}()

	service, err := services.NewProtocolMigrateHistoryService(services.ProtocolMigrateHistoryConfig{
		DB:                     dbPool,
		LedgerBackend:          ledgerBackend,
		ProtocolsModel:         models.Protocols,
		ProtocolContractsModel: models.ProtocolContracts,
		IngestStore:            models.IngestStore,
		NetworkPassphrase:      networkPassphrase,
		Processors:             processors,
		LatestLedgerCursorName: latestLedgerCursorName,
		OldestLedgerCursorName: oldestLedgerCursorName,
	})
	if err != nil {
		return fmt.Errorf("creating protocol migrate history service: %w", err)
	}

	if err := service.Run(ctx, protocolIDs); err != nil {
		return fmt.Errorf("running protocol migrate history: %w", err)
	}

	return nil
}
