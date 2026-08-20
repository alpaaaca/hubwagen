package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	Version   = "v0.1.0"
	CommitSHA = "dev"
	BuildDate = "unknown"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("hubwagen %s (commit: %s, built: %s)\n", Version, CommitSHA, BuildDate)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
