package cmd

import (
	"context"
	"fmt"
	"go/types"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/datamigrations"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/ingest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

type dataMigrateCmd struct{}

func (c *dataMigrateCmd) Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "data-migrate",
		Short: "Rewrite history rows ingested before a release changed what the indexer writes",
		Long:  "Parent command for data migrations. The live ingester records each migration with its boundary ledger; `run` rewrites the ledgers below it and `status` shows progress.",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cmd.Help(); err != nil {
				log.Fatalf("Error calling help command: %s", err.Error())
			}
		},
	}

	cmd.AddCommand(c.runCommand())
	cmd.AddCommand(c.statusCommand())

	return cmd
}

func (c *dataMigrateCmd) runCommand() *cobra.Command {
	var (
		databaseURL       string
		networkPassphrase string
		logLevel          logrus.Level
		rpcURL            string
		ledgerBackendType string
		getLedgersLimit   int
		datastore         ingest.DatastoreConfig
		metricsPort       int
	)

	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&databaseURL),
		utils.NetworkPassphraseOption(&networkPassphrase),
		utils.LogLevelOption(&logLevel),
		{
			Name:        "rpc-url",
			Usage:       "The URL of the RPC Server. Required when --ledger-backend-type=rpc.",
			OptType:     types.String,
			ConfigKey:   &rpcURL,
			FlagDefault: "",
			Required:    false,
		},
		{
			Name:        "ledger-backend-type",
			Usage:       "Where historical ledgers are read from: 'rpc' (default) or 'datastore'. Use datastore with datastore-bucket-path for ledgers outside the RPC retention window.",
			OptType:     types.String,
			ConfigKey:   &ledgerBackendType,
			FlagDefault: string(ingest.LedgerBackendTypeRPC),
			Required:    false,
		},
		utils.GetLedgersLimitOption(&getLedgersLimit),
	}
	cfgOpts = append(cfgOpts, utils.DatastoreOptions(&datastore)...)

	cmd := &cobra.Command{
		Use:   "run <name>",
		Short: "Run a data migration to completion",
		Long: "Rewrites the named migration's rows for every ledger below its boundary, newest first, while ingestion and the API keep running. " +
			"Resumes from the recorded cursor after an interruption. Only one data migration runs at a time.",
		Args: cobra.ExactArgs(1),
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}
			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}
			log.DefaultLogger.SetLevel(logLevel)

			switch ledgerBackendType {
			case string(ingest.LedgerBackendTypeRPC):
				if rpcURL == "" {
					return fmt.Errorf("--rpc-url is required when --ledger-backend-type=rpc")
				}
			case string(ingest.LedgerBackendTypeDatastore):
				if datastore.BucketPath == "" {
					return fmt.Errorf("--datastore-bucket-path is required when --ledger-backend-type=datastore")
				}
			default:
				return fmt.Errorf("invalid --ledger-backend-type %q, must be 'rpc' or 'datastore'", ledgerBackendType)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, ok := datamigrations.Find(name); !ok {
				names := make([]string, 0, len(datamigrations.All))
				for _, m := range datamigrations.All {
					names = append(names, m.Name())
				}
				return fmt.Errorf("unknown data migration %q, known: %s", name, strings.Join(names, ", "))
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			dbPool, err := db.OpenDBConnectionPool(ctx, databaseURL)
			if err != nil {
				return fmt.Errorf("opening database connection: %w", err)
			}
			defer dbPool.Close()

			m := metrics.NewMetrics(prometheus.NewRegistry())
			models, err := data.NewModels(dbPool, m.DB)
			if err != nil {
				return fmt.Errorf("creating models: %w", err)
			}

			if metricsPort > 0 {
				metricsServer := startMigrationMetricsServer(metricsPort, m.Registry())
				defer func() {
					shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if shErr := metricsServer.Shutdown(shutdownCtx); shErr != nil {
						log.Ctx(ctx).Warnf("error shutting down metrics server: %v", shErr)
					}
				}()
			}

			pool := pond.NewPool(runtime.GOMAXPROCS(0))
			defer pool.StopAndWait()

			runner := &datamigrations.Runner{
				Deps: datamigrations.Deps{
					Models:            models,
					NetworkPassphrase: networkPassphrase,
					Pool:              pool,
				},
				BackendFactory: func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
					backend, err := ingest.NewLedgerBackend(ctx, ingest.Configs{
						LedgerBackendType: ingest.LedgerBackendType(ledgerBackendType),
						Datastore:         datastore,
						NetworkPassphrase: networkPassphrase,
						RPCURL:            rpcURL,
						GetLedgersLimit:   getLedgersLimit,
					})
					if err != nil {
						return nil, fmt.Errorf("creating ledger backend: %w", err)
					}
					return backend, nil
				},
				Metrics: m.DataMigration,
			}
			if err := runner.Run(ctx, name); err != nil {
				return fmt.Errorf("running data migration %s: %w", name, err)
			}
			return nil
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}
	cmd.Flags().IntVar(&metricsPort, "metrics-port", 0, "Port to expose Prometheus /metrics on. 0 disables (default).")

	return cmd
}

func (c *dataMigrateCmd) statusCommand() *cobra.Command {
	var databaseURL string

	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&databaseURL),
	}

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the progress of every data migration this binary knows",
		Args:  cobra.NoArgs,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}
			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			dbPool, err := db.OpenDBConnectionPool(ctx, databaseURL)
			if err != nil {
				return fmt.Errorf("opening database connection: %w", err)
			}
			defer dbPool.Close()

			models, err := data.NewModels(dbPool, metrics.NewMetrics(prometheus.NewRegistry()).DB)
			if err != nil {
				return fmt.Errorf("creating models: %w", err)
			}
			rows, err := models.DataMigrations.List(ctx)
			if err != nil {
				return fmt.Errorf("listing data migrations: %w", err)
			}
			floor, err := models.IngestStore.Get(ctx, data.OldestLedgerCursorName)
			if err != nil {
				return fmt.Errorf("reading oldest ingested ledger: %w", err)
			}

			recorded := make(map[string]data.DataMigration, len(rows))
			for _, row := range rows {
				recorded[row.Name] = row
			}

			lines := []string{"NAME\tSTATUS\tBOUNDARY\tCURSOR\tFLOOR\tSTARTED\tFINISHED\tLAST ERROR"}
			for _, m := range datamigrations.All {
				row, ok := recorded[m.Name()]
				if !ok {
					lines = append(lines, fmt.Sprintf("%s\tnot recorded\t-\t-\t%d\t-\t-\t-", m.Name(), floor))
					continue
				}
				lastError := "-"
				if row.LastError != nil {
					lastError = *row.LastError
				}
				lines = append(lines, fmt.Sprintf("%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s",
					row.Name, row.Status, row.BoundaryLedger, row.CursorLedger, floor,
					formatOptionalTime(row.StartedAt), formatOptionalTime(row.FinishedAt), lastError))
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			for _, line := range lines {
				if _, err := fmt.Fprintln(w, line); err != nil {
					return fmt.Errorf("writing status: %w", err)
				}
			}
			if err := w.Flush(); err != nil {
				return fmt.Errorf("writing status: %w", err)
			}
			return nil
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	return cmd
}

// formatOptionalTime renders t in RFC 3339, or "-" when it is unset.
func formatOptionalTime(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}
