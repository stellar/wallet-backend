// Package cmd provides the CLI commands for wallet-backend.
// This file contains the loadtest run command for running synthetic ledger ingestion.
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
		Long: `Load testing utilities for running synthetic ledger ingestion.
These tools help measure and validate the ingestion pipeline's throughput and reliability.`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := cmd.Help(); err != nil {
				log.Fatalf("Error calling help command: %s", err.Error())
			}
		},
	}

	cmd.AddCommand(c.runCommand())

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
		StartLedger         int
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
			Usage:       "Port for the loadtest metrics server",
			OptType:     types.Int,
			ConfigKey:   &cfg.ServerPort,
			FlagDefault: 8003,
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
		{
			Name:        "start-ledger",
			Usage:       "Ledger sequence to start ingestion from (defaults to 606644043 for pubnet datastore)",
			OptType:     types.Int,
			ConfigKey:   &cfg.StartLedger,
			FlagDefault: 606644043,
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
			log.Infof("  Start ledger: %d", cfg.StartLedger)

			err := loadtest.Run(ctx, loadtest.RunConfig{
				LedgersFilePath:     cfg.LedgersFilePath,
				LedgerCloseDuration: cfg.LedgerCloseDuration,
				DatabaseURL:         cfg.DatabaseURL,
				NetworkPassphrase:   cfg.NetworkPassphrase,
				ServerPort:          cfg.ServerPort,
				SkipTxMeta:          cfg.SkipTxMeta,
				SkipTxEnvelope:      cfg.SkipTxEnvelope,
				StartLedger:         uint32(cfg.StartLedger),
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
