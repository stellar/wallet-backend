package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go/support/log"
)

// Version is the official version of this application.
const Version = "0.1.0"

// GitCommit is populated at build time by
// go build -ldflags "-X main.GitCommit=$GIT_COMMIT"
var GitCommit string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:           "wallet-backend",
	Short:         "Wallet Backend Server",
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		err := cmd.Help()
		if err != nil {
			return fmt.Errorf("calling help command: %w", err)
		}
		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	SetupCLI()
	err := rootCmd.Execute()
	if err != nil {
		panic(fmt.Errorf("executing root command: %w", err))
	}
}

func preConfigureLogger() {
	log.DefaultLogger = log.New()
	log.DefaultLogger.SetLevel(logrus.TraceLevel)
}

func SetupCLI() {
	preConfigureLogger()
	log.Info("Version: ", Version)
	log.Info("GitCommit: ", GitCommit)

	rootCmd.AddCommand((&serveCmd{}).Command())
	rootCmd.AddCommand((&ingestCmd{}).Command())
	rootCmd.AddCommand((&migrateCmd{}).Command())
	rootCmd.AddCommand((&channelAccountCmd{}).Command(&ChAccCmdService{}))
	rootCmd.AddCommand((&distributionAccountCmd{}).Command())
}
