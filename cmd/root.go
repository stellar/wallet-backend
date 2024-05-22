package cmd

import (
	"log"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	supportlog "github.com/stellar/go/support/log"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "wallet-backend",
	Short: "Wallet Backend Server",
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
	err := rootCmd.Execute()
	if err != nil {
		log.Fatalf("Error executing root command: %s", err.Error())
	}
}

func init() {
	logger := supportlog.New()
	logger.SetLevel(logrus.TraceLevel)

	rootCmd.AddCommand((&serveCmd{Logger: logger}).Command())
	rootCmd.AddCommand((&ingestCmd{Logger: logger}).Command())
}
