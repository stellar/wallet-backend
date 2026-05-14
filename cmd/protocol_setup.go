package cmd

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/alitto/pond/v2"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	_ "github.com/stellar/wallet-backend/internal/services/sep41" // registers SEP-41 validator + processor via init()
)

type protocolSetupCmd struct{}

func (c *protocolSetupCmd) Command() *cobra.Command {
	var databaseURL string
	var rpcURL string
	var networkPassphrase string
	var protocolIDs []string
	var logLevel string

	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&databaseURL),
		utils.RPCURLOption(&rpcURL),
		utils.NetworkPassphraseOption(&networkPassphrase),
	}

	cmd := &cobra.Command{
		Use:   "protocol-setup",
		Short: "Classify WASMs by protocol using bytecode analysis",
		Long:  "Fetches unclassified WASM bytecodes via RPC and classifies them against registered protocol validators.",
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
			return c.Run(databaseURL, rpcURL, networkPassphrase, protocolIDs)
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	cmd.Flags().StringSliceVar(&protocolIDs, "protocol-id", nil, "Protocol ID(s) to classify (required, repeatable)")
	cmd.Flags().StringVar(&logLevel, "log-level", "", `Log level: "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "PANIC"`)

	return cmd
}

func (c *protocolSetupCmd) Run(databaseURL, rpcURL, networkPassphrase string, protocolIDs []string) error {
	ctx := context.Background()

	// Open DB connection
	dbPool, err := db.OpenDBConnectionPool(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("opening database connection: %w", err)
	}
	defer dbPool.Close()

	// Run protocol migrations to ensure protocols are registered in the DB
	if _, err := db.RunProtocolMigrations(ctx, dbPool); err != nil {
		return fmt.Errorf("running protocol migrations: %w", err)
	}

	// Create models
	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(dbPool, m.DB)
	if err != nil {
		return fmt.Errorf("creating models: %w", err)
	}

	// Create RPC service
	httpClient := &http.Client{Timeout: 30 * time.Second}
	rpcService, err := services.NewRPCService(rpcURL, networkPassphrase, httpClient, m.RPC)
	if err != nil {
		return fmt.Errorf("creating RPC service: %w", err)
	}

	// Build the contract metadata service. Per-protocol validators that need
	// it (e.g. SEP-41) pull it from ProtocolDeps; the framework itself is
	// agnostic.
	metadataPool := pond.NewPool(0)
	defer metadataPool.StopAndWait()
	metadataService, err := services.NewContractMetadataService(rpcService, models.Contract, metadataPool)
	if err != nil {
		return fmt.Errorf("creating contract metadata service: %w", err)
	}

	// Build validators via the registry. Sort the requested IDs so the
	// dispatcher's first-match-wins iteration is deterministic.
	deps := services.ProtocolDeps{
		NetworkPassphrase:       networkPassphrase,
		Models:                  models,
		RPCService:              rpcService,
		ContractMetadataService: metadataService,
		MetricsService:          m,
	}
	sortedIDs := append([]string(nil), protocolIDs...)
	sort.Strings(sortedIDs)
	validators, err := services.BuildValidators(deps, sortedIDs)
	if err != nil {
		return fmt.Errorf("building validators: %w", err)
	}

	// Create spec extractor — owned by the service, closed via service.Run.
	specExtractor := services.NewWasmSpecExtractor()

	service := services.NewProtocolSetupService(
		dbPool,
		rpcService,
		models,
		specExtractor,
		validators,
	)

	if err := service.Run(ctx, protocolIDs); err != nil {
		return fmt.Errorf("running protocol setup: %w", err)
	}

	return nil
}
