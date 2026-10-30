package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var logLevel, logFormat string

var rootCmd = &cobra.Command{
	Use:   "hubwagen",
	Short: "Durable execution for data: pipelines as code, steps reused when nothing changed, every output a saved version",
	PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
		return configureLogging(logLevel, logFormat)
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level (debug|info|warn|error)")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "log format (text|json)")
}

// utcTime logs times in UTC, like every time hubwagen shows.
func utcTime(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey && len(groups) == 0 && a.Value.Kind() == slog.KindTime {
		a.Value = slog.TimeValue(a.Value.Time().UTC())
	}
	return a
}

// configureLogging sets the default slog logger from --log-level and
// --log-format. Structured logs go to stderr so they never interleave with
// command output and task logs on stdout.
func configureLogging(level, format string) error {
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
	opts := &slog.HandlerOptions{Level: lvl, ReplaceAttr: utcTime}
	switch strings.ToLower(format) {
	case "text", "":
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, opts)))
	case "json":
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, opts)))
	default:
		return fmt.Errorf("invalid --log-format %q (want text|json)", format)
	}
	return nil
}
