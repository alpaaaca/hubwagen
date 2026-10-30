package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/alper/hubwagen/internal/config"
	"github.com/alper/hubwagen/internal/server"
	"github.com/alper/hubwagen/internal/webui"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the hubwagen server on its own, as a long-running service",
	Long: `Server runs the hubwagen server: its state, run histories, step logs and
data versions in --data-dir, the API and web UI on --listen. It is the
server of hubwagen dev without the rest: it starts no workers and prints no
runs, only its logs (stderr; --log-format json for a log collector). Run it
under a service manager (systemd, launchd, a container) and the workers
next to it, with hubwagen workers --server URL or the same service manager.

Settings come from hubwagen.yaml (--config, $HUBWAGEN_CONFIG, else
./hubwagen.yaml if present); flags override it. The workers: section is
not used.

GET /healthz answers 200 while the server runs and 503 once it has
stopped. SIGTERM or Ctrl-C stops it: the API closes, polls in flight end,
and work that workers are running carries on and is picked up on the next
start. If the server stops on its own (it couldn't write its state), it
exits with an error, for the service manager to restart it.

The API has no authentication yet: keep --listen on a loopback address, or
reachable only by people and workers you trust.

Only one process can use a data directory at a time.`,
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE:         runServer,
}

func init() {
	serverCmd.Flags().StringVar(&dataDir, "data-dir", config.DefaultDataDir, "directory for the server's state, logbooks and pallets (default: from the config)")
	serverCmd.Flags().StringVar(&listenAddr, "listen", config.DefaultListen, "address to serve the API on (default: from the config)")
	addConfigFlag(serverCmd)
	rootCmd.AddCommand(serverCmd)
}

func runServer(cmd *cobra.Command, _ []string) error {
	cfg, err := loadConfig(cmd)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := slog.Default()
	e, err := server.Start(ctx, server.Config{DataDir: cfg.DataDir, Listen: cfg.Server.Listen, Logger: log})
	if err != nil {
		return err
	}
	defer e.Close()
	log.Info("hubwagen server started", "version", Version, "commit", CommitSHA, "config", configName(cfg), "data_dir", e.DataDir,
		"listen", e.APIAddr, "web_ui", webui.Built())
	if !loopback(cfg.Server.Listen) {
		log.Warn("the API has no authentication: anyone who can reach it can start, cancel and read runs", "listen", cfg.Server.Listen)
	}
	if n := len(cfg.Workers); n > 0 {
		log.Info("hubwagen server starts no workers; run the config's with hubwagen workers", "groups", n)
	}

	select {
	case <-ctx.Done():
		log.Info("stopping")
		if err := e.Close(); err != nil {
			return err
		}
		log.Info("stopped")
		return nil
	case err := <-e.Done():
		return fmt.Errorf("server stopped: %w", err)
	}
}

// loopback reports whether listen only accepts connections from this
// machine: a loopback IP or localhost.
func loopback(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
