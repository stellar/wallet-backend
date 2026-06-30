package cmd

import (
	"context"
	"fmt"
	"strconv"

	migrate "github.com/rubenv/sql-migrate"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/db"
)

type migrateCmd struct{}

func (c *migrateCmd) Command() *cobra.Command {
	var databaseURL string
	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&databaseURL),
	}

	migrateCmd := &cobra.Command{
		Use:               "migrate",
		Short:             "Schema migration helpers",
		PersistentPreRunE: utils.DefaultPersistentPreRunE(cfgOpts),
	}

	migrateUpCmd := &cobra.Command{
		Use:   "up",
		Short: "Applies all pending schema migrations and registers protocols",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return c.RunMigrateUp(cmd.Context(), databaseURL)
		},
	}

	migrateDownCmd := &cobra.Command{
		Use:   "down [count]",
		Short: "Migrates database down [count] migrations",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.RunMigrateDown(cmd.Context(), databaseURL, args)
		},
	}

	migrateCmd.AddCommand(migrateUpCmd)
	migrateCmd.AddCommand(migrateDownCmd)

	if err := cfgOpts.Init(migrateCmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	return migrateCmd
}

func (c *migrateCmd) RunMigrateUp(ctx context.Context, databaseURL string) error {
	// migrate up applies all pending schema migrations, then registers protocols so live
	// ingestion can classify new-protocol WASMs immediately on restart, without waiting for
	// protocol-setup. Protocol SQL is idempotent (ON CONFLICT DO NOTHING).
	if err := executeMigrations(ctx, databaseURL, migrate.Up, 0); err != nil {
		return fmt.Errorf("executing migrate up: %w", err)
	}

	pool, err := db.OpenDBConnectionPool(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("opening database connection pool for protocol migrations: %w", err)
	}
	defer pool.Close()
	if _, err := db.RunProtocolMigrations(ctx, pool); err != nil {
		return fmt.Errorf("registering protocols: %w", err)
	}
	return nil
}

func (c *migrateCmd) RunMigrateDown(ctx context.Context, databaseURL string, args []string) error {
	count, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("invalid [count] argument: %s", args[0])
	}
	if err := executeMigrations(ctx, databaseURL, migrate.Down, count); err != nil {
		return fmt.Errorf("executing migrate down: %w", err)
	}
	return nil
}

func executeMigrations(ctx context.Context, databaseURL string, direction migrate.MigrationDirection, count int) error {
	numMigrationsRun, err := db.Migrate(ctx, databaseURL, direction, count)
	if err != nil {
		return fmt.Errorf("migrating database: %w", err)
	}

	if numMigrationsRun == 0 {
		log.Ctx(ctx).Info("No migrations applied.")
	} else {
		log.Ctx(ctx).Infof("Successfully applied %d migrations %s.", numMigrationsRun, migrationDirectionStr(direction))
	}
	return nil
}

func migrationDirectionStr(direction migrate.MigrationDirection) string {
	if direction == migrate.Up {
		return "up"
	}
	return "down"
}
