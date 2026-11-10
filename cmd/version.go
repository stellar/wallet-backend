package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

// versionCommand prints the build identity. Version and GitCommit are injected
// through ldflags (see main.go); the Go version comes from the toolchain that
// built the binary.
func versionCommand(cfg RootConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the wallet-backend version and build commit",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			commit := cfg.GitCommit
			if commit == "" {
				commit = "unknown"
			}
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "wallet-backend %s (commit %s, %s)\n", cfg.Version, commit, runtime.Version()); err != nil {
				return fmt.Errorf("writing version: %w", err)
			}
			return nil
		},
	}
}
