package cmd

import (
	"context"
	"fmt"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
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

// validatorFactories maps protocol ID constants to their validator constructors.
var validatorFactories = map[string]func() services.ProtocolValidator{
	// Validators will be registered here as they are implemented.
	// Example: data.ProtocolSEP41: services.NewSEP41Validator,
}

func (c *protocolSetupCmd) Run(databaseURL, rpcURL, networkPassphrase string, protocolIDs []string) error {
	ctx := context.Background()

	// Build validators from protocol IDs
	var validators []services.ProtocolValidator
	for _, pid := range protocolIDs {
		factory, ok := validatorFactories[pid]
		if !ok {
			return fmt.Errorf("unknown protocol ID %q — no validator registered", pid)
		}
		validators = append(validators, factory())
	}

	// Open DB connection
	dbPool, err := db.OpenDBConnectionPool(databaseURL)
	if err != nil {
		return fmt.Errorf("opening database connection: %w", err)
	}
	defer dbPool.Close()

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

	// Create RPC service
	httpClient := &http.Client{Timeout: 30 * time.Second}
	rpcService, err := services.NewRPCService(rpcURL, networkPassphrase, httpClient, metricsService)
	if err != nil {
		return fmt.Errorf("creating RPC service: %w", err)
	}

	// Create spec extractor
	specExtractor := services.NewWasmSpecExtractor()

	// Create and run the service
	service := services.NewProtocolSetupService(
		dbPool,
		rpcService,
		models.Protocol,
		models.ProtocolWasm,
		specExtractor,
		validators,
	)

	if err := service.Run(ctx, protocolIDs); err != nil {
		return fmt.Errorf("running protocol setup: %w", err)
	}

	return nil
}
