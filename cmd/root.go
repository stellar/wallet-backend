package cmd

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/support/log"
)

type RootConfig struct {
	GitCommit string
	Version   string
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:           "wallet-backend",
	Short:         "Wallet Backend Server",
	SilenceErrors: true,
	SilenceUsage:  true,
	Run: func(cmd *cobra.Command, args []string) {
		err := cmd.Help()
		if err != nil {
			log.Fatalf("Error calling help command: %s", err.Error())
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(cfg RootConfig) {
	SetupCLI(cfg)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func preConfigureLogger() {
	log.DefaultLogger = log.New()
	log.DefaultLogger.SetLevel(logrus.TraceLevel)
}

func SetupCLI(cfg RootConfig) {
	preConfigureLogger()
	// Subcommands define their own PersistentPreRunE; without this, cobra would
	// run only the nearest hook and skip the root one below.
	cobra.EnableTraverseRunHooks = true
	rootCmd.PersistentPreRun = func(cmd *cobra.Command, _ []string) {
		// Every service command logs its build identity on startup. The version
		// command prints it as its output instead.
		if cmd.Name() == "version" {
			return
		}
		commit := cfg.GitCommit
		if commit == "" {
			commit = "unknown"
		}
		log.DefaultLogger.Infof("wallet-backend %s (commit %s)", cfg.Version, commit)
	}

	rootCmd.AddCommand(versionCommand(cfg))
	rootCmd.AddCommand((&serveCmd{}).Command())
	rootCmd.AddCommand((&ingestCmd{}).Command())
	rootCmd.AddCommand((&migrateCmd{}).Command())
	rootCmd.AddCommand((&protocolSetupCmd{}).Command())
	rootCmd.AddCommand((&protocolMigrateCmd{}).Command())
}
