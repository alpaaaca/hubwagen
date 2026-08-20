package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var (
	dbPath   string
	logLevel string
)

var rootCmd = &cobra.Command{
	Use:   "hubwagen",
	Short: "Lightweight data pipeline scheduler",
	PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
		return configureLogging(logLevel)
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&dbPath, "db", "./hubwagen.db", "path to SQLite database file")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level (debug|info|warn|error)")
}

// configureLogging sets the default slog logger's level from --log-level.
// Structured logs go to stderr so they never interleave with the DAG's own
// stdout output (step logs, ✓/✗ summary).
func configureLogging(level string) error {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "info", "":
		lvl = slog.LevelInfo
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		return fmt.Errorf("invalid --log-level %q (want debug|info|warn|error)", level)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))
	return nil
}
