// Package cmd provides the CLI commands for wallet-backend.
// This file contains the loadtest command for generating and running synthetic ledger ingestion.
package cmd

import (
	"context"
	"fmt"
	"go/types"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/loadtest"
)

type loadtestCmd struct{}

func (c *loadtestCmd) Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "loadtest",
		Short: "Load testing utilities for wallet-backend",
		Long: `Load testing utilities for generating synthetic ledgers and testing
ingestion performance. These tools help measure and validate the ingestion
pipeline's throughput and reliability.`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := cmd.Help(); err != nil {
				log.Fatalf("Error calling help command: %s", err.Error())
			}
		},
	}

	cmd.AddCommand(c.generateCommand())
	cmd.AddCommand(c.runCommand())

	return cmd
}

func (c *loadtestCmd) generateCommand() *cobra.Command {
	cfg := struct {
		TransactionsPerLedger int
		TransfersPerTx        int
		LedgerCount           int
		OutputPath            string
	}{}

	cfgOpts := config.ConfigOptions{
		{
			Name:        "transactions-per-ledger",
			Usage:       "Number of transactions per generated ledger (must be multiple of 100)",
			OptType:     types.Int,
			ConfigKey:   &cfg.TransactionsPerLedger,
			FlagDefault: 100,
			Required:    false,
		},
		{
			Name:        "transfers-per-tx",
			Usage:       "Number of token transfers per bulk transaction",
			OptType:     types.Int,
			ConfigKey:   &cfg.TransfersPerTx,
			FlagDefault: 10,
			Required:    false,
		},
		{
			Name:        "ledger-count",
			Usage:       "Number of ledgers to generate",
			OptType:     types.Int,
			ConfigKey:   &cfg.LedgerCount,
			FlagDefault: 2,
			Required:    false,
		},
		{
			Name:        "output-path",
			Usage:       "Output path for the generated zstd-compressed XDR ledgers file",
			OptType:     types.String,
			ConfigKey:   &cfg.OutputPath,
			FlagDefault: "./testdata/synthetic_ledgers.xdr.zstd",
			Required:    true,
		},
	}

	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate synthetic ledgers for load testing",
		Long: `Generate synthetic ledgers with bulk Soroban token transfers for load testing.
This command starts Docker containers (Stellar Core + RPC) in standalone mode,
creates accounts, deploys contracts, submits bulk transfer transactions, and
writes the resulting ledgers to a zstd-compressed XDR file.

The generated file is compatible with github.com/stellar/go/ingest/loadtest.LedgerBackend
and can be used to test ingestion performance without requiring a live network.

Requirements:
  - Docker must be running
  - Ports 11625, 11626, 8000 should be available

Example:
  wallet-backend loadtest generate \
    --transactions-per-ledger 100 \
    --transfers-per-tx 10 \
    --ledger-count 10 \
    --output ./testdata/synthetic_ledgers.xdr.zstd
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("validating required config: %w", err)
			}
			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting config values: %w", err)
			}

			// Setup context with signal handling
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			signalChan := make(chan os.Signal, 1)
			signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
			go func() {
				<-signalChan
				log.Info("Received shutdown signal, cleaning up...")
				cancel()
			}()

			log.Info("Starting synthetic ledger generation...")
			log.Infof("  Transactions per ledger: %d", cfg.TransactionsPerLedger)
			log.Infof("  Transfers per transaction: %d", cfg.TransfersPerTx)
			log.Infof("  Ledger count: %d", cfg.LedgerCount)
			log.Infof("  Output path: %s", cfg.OutputPath)

			err := loadtest.Generate(ctx, loadtest.GeneratorConfig{
				TransactionsPerLedger: cfg.TransactionsPerLedger,
				TransfersPerTx:        cfg.TransfersPerTx,
				LedgerCount:           cfg.LedgerCount,
				OutputPath:            cfg.OutputPath,
			})
			if err != nil {
				log.Errorf("Ledger generation failed: %v", err)
				return fmt.Errorf("generating ledgers: %w", err)
			}

			log.Infof("Successfully generated ledgers to %s", cfg.OutputPath)
			return nil
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing config options: %s", err.Error())
	}

	return cmd
}

func (c *loadtestCmd) runCommand() *cobra.Command {
	cfg := struct {
		LedgersFilePath     string
		LedgerCloseDuration time.Duration
		DatabaseURL         string
		NetworkPassphrase   string
		ServerPort          int
		AdminPort           int
		SkipTxMeta          bool
		SkipTxEnvelope      bool
	}{}

	cfgOpts := config.ConfigOptions{
		{
			Name:        "ledgers-file-path",
			Usage:       "Path to zstd-compressed XDR file containing synthetic ledgers",
			OptType:     types.String,
			ConfigKey:   &cfg.LedgersFilePath,
			FlagDefault: "",
			Required:    true,
		},
		{
			Name:        "ledger-close-duration",
			Usage:       "Simulated duration between ledger closes (e.g., '5s'). Set to 0 for maximum throughput.",
			OptType:     types.String,
			ConfigKey:   &cfg.LedgerCloseDuration,
			FlagDefault: "0s",
			Required:    false,
			CustomSetValue: func(co *config.ConfigOption) error {
				durationStr := viper.GetString(co.Name)
				d, err := time.ParseDuration(durationStr)
				if err != nil {
					return fmt.Errorf("invalid duration in %s: %w", co.Name, err)
				}
				key, ok := co.ConfigKey.(*time.Duration)
				if !ok {
					return fmt.Errorf("the expected type for the config key in %s is *time.Duration, but a %T was provided instead", co.Name, co.ConfigKey)
				}
				*key = d
				return nil
			},
		},
		{
			Name:        "database-url",
			Usage:       "PostgreSQL connection URL",
			OptType:     types.String,
			ConfigKey:   &cfg.DatabaseURL,
			FlagDefault: "",
			Required:    true,
		},
		{
			Name:        "network-passphrase",
			Usage:       "Stellar network passphrase",
			OptType:     types.String,
			ConfigKey:   &cfg.NetworkPassphrase,
			FlagDefault: "",
			Required:    true,
		},
		{
			Name:        "server-port",
			Usage:       "Port for metrics server",
			OptType:     types.Int,
			ConfigKey:   &cfg.ServerPort,
			FlagDefault: 8003,
			Required:    false,
		},
		{
			Name:        "admin-port",
			Usage:       "Port for admin/pprof server (0 to disable)",
			OptType:     types.Int,
			ConfigKey:   &cfg.AdminPort,
			FlagDefault: 0,
			Required:    false,
		},
		{
			Name:        "skip-tx-meta",
			Usage:       "Skip storing transaction metadata (meta_xdr)",
			OptType:     types.Bool,
			ConfigKey:   &cfg.SkipTxMeta,
			FlagDefault: true,
			Required:    false,
		},
		{
			Name:        "skip-tx-envelope",
			Usage:       "Skip storing transaction envelope (envelope_xdr)",
			OptType:     types.Bool,
			ConfigKey:   &cfg.SkipTxEnvelope,
			FlagDefault: true,
			Required:    false,
		},
	}

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run ingestion from synthetic ledgers file",
		Long: `Run ingestion from a zstd-compressed XDR file containing synthetic ledgers.
This command processes ledgers from the file and writes them to the database,
measuring ingestion performance without requiring a live network.

The ledgers file should be generated using the 'loadtest generate' command.

Example:
  wallet-backend loadtest run \
    --ledgers-file-path ./testdata/synthetic_ledgers.xdr.zstd \
    --database-url "postgres://postgres@localhost:5432/wallet-backend?sslmode=disable" \
    --network-passphrase "Test SDF Network ; September 2015" \
    --server-port 8003
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("validating required config: %w", err)
			}
			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting config values: %w", err)
			}

			// Setup context with signal handling
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			signalChan := make(chan os.Signal, 1)
			signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
			go func() {
				<-signalChan
				log.Info("Received shutdown signal, cleaning up...")
				cancel()
			}()

			log.Info("Starting loadtest ingestion run...")
			log.Infof("  Ledgers file: %s", cfg.LedgersFilePath)
			log.Infof("  Ledger close duration: %v", cfg.LedgerCloseDuration)
			log.Infof("  Database URL: %s", cfg.DatabaseURL)
			log.Infof("  Network passphrase: %s", cfg.NetworkPassphrase)
			log.Infof("  Server port: %d", cfg.ServerPort)

			err := loadtest.Run(ctx, loadtest.RunConfig{
				LedgersFilePath:     cfg.LedgersFilePath,
				LedgerCloseDuration: cfg.LedgerCloseDuration,
				DatabaseURL:         cfg.DatabaseURL,
				NetworkPassphrase:   cfg.NetworkPassphrase,
				ServerPort:          cfg.ServerPort,
				AdminPort:           cfg.AdminPort,
				SkipTxMeta:          cfg.SkipTxMeta,
				SkipTxEnvelope:      cfg.SkipTxEnvelope,
			})
			if err != nil {
				log.Errorf("Loadtest run failed: %v", err)
				return fmt.Errorf("running loadtest: %w", err)
			}

			return nil
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing config options: %s", err.Error())
	}

	return cmd
}
