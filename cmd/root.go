package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go/support/log"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:           "wallet-backend",
	Short:         "Wallet Backend Server",
	SilenceErrors: true,
	Run: func(cmd *cobra.Command, args []string) {
		err := cmd.Help()
		if err != nil {
			log.Fatalf("Error calling help command: %s", err.Error())
		}
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

	rootCmd.AddCommand((&serveCmd{}).Command())
	rootCmd.AddCommand((&ingestCmd{}).Command())
	rootCmd.AddCommand((&migrateCmd{}).Command())
	rootCmd.AddCommand((&channelAccountCmd{}).Command(&ChAccCmdService{}))
	rootCmd.AddCommand((&distributionAccountCmd{}).Command())
}
