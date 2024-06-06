package cmd

import (
	"context"
	"fmt"
	"go/types"
	"strconv"

	migrate "github.com/rubenv/sql-migrate"
	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/db"
)

type migrateCmd struct{}

func (c *migrateCmd) Command() *cobra.Command {
	var databaseURL string
	cfgOpts := config.ConfigOptions{
		{
			Name:        "database-url",
			Usage:       "Database connection URL",
			OptType:     types.String,
			ConfigKey:   &databaseURL,
			FlagDefault: "postgres://postgres@localhost:5432/wallet-backend?sslmode=disable",
			Required:    true,
		},
	}

	migrateCmd := &cobra.Command{
		Use:   "migrate",
		Short: "Schema migration helpers",
		PersistentPreRun: func(_ *cobra.Command, _ []string) {
			cfgOpts.Require()
			if err := cfgOpts.SetValues(); err != nil {
				log.Fatalf("Error setting values of config options: %s", err.Error())
			}
		},
	}

	migrateUpCmd := cobra.Command{
		Use:   "up",
		Short: "Migrates database up [count] migrations",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var count int
			if len(args) > 0 {
				var err error
				count, err = strconv.Atoi(args[0])
				if err != nil {
					log.Fatalf("Invalid [count] argument: %s", args[0])
				}
			}

			if err := executeMigrations(cmd.Context(), databaseURL, migrate.Up, count); err != nil {
				log.Fatalf("Error executing migrate up: %v", err)
			}
		},
	}

	migrateDownCmd := &cobra.Command{
		Use:   "down [count]",
		Short: "Migrates database down [count] migrations",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			count, err := strconv.Atoi(args[0])
			if err != nil {
				log.Fatalf("Invalid [count] argument: %s", args[0])
			}

			if err := executeMigrations(cmd.Context(), databaseURL, migrate.Down, count); err != nil {
				log.Fatalf("Error executing migrate down: %v", err)
			}
		},
	}

	migrateCmd.AddCommand(&migrateUpCmd)
	migrateCmd.AddCommand(migrateDownCmd)

	if err := cfgOpts.Init(migrateCmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	return migrateCmd
}

func executeMigrations(ctx context.Context, databaseURL string, direction migrate.MigrationDirection, count int) error {
	numMigrationsRun, err := db.Migrate(databaseURL, direction, count)
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
