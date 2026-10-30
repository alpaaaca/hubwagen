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
	"github.com/alper/hubwagen/internal/supervise"
	"github.com/alper/hubwagen/internal/webui"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the hubwagen server on its own, as a long-running service",
	Long: `Server runs the hubwagen server: its state, run histories, step logs and
data versions in --data-dir, the API and web UI on --listen. It is the
server of hubwagen dev for a service manager (systemd, launchd, a
container): it prints no runs, only its logs (stderr; --log-format json
for a log collector), with the workers' output in them.

Settings come from hubwagen.yaml (--config, $HUBWAGEN_CONFIG, else
./hubwagen.yaml if present); flags override it. The worker processes listed
under workers: are started once the API is up and restarted if they exit,
as hubwagen dev does, so queued runs don't wait for workers nobody starts.
--no-workers starts the server alone, for workers run elsewhere (hubwagen
workers --server URL, the service manager, another machine).

GET /healthz answers 200 while the server runs and 503 once it has
stopped. SIGTERM or Ctrl-C stops the workers first (running steps finish,
polls end within their wait; a second signal kills them), then the server:
the API closes, and work that workers elsewhere are running carries on and
is picked up on the next start. If the server stops on its own (it
couldn't write its state), it stops its workers and exits with an error,
for the service manager to restart it.

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
	serverCmd.Flags().BoolVar(&noWorkers, "no-workers", false, "don't start the workers from the config (they run elsewhere)")
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
	scfg, err := serverConfig(cfg)
	if err != nil {
		return err
	}
	scfg.Logger = log
	e, err := server.Start(ctx, scfg)
	if err != nil {
		return err
	}
	defer e.Close()
	log.Info("hubwagen server started", "version", Version, "commit", CommitSHA, "config", configName(cfg), "data_dir", e.DataDir,
		"bucket", e.Bucket.Base(), "listen", e.APIAddr, "web_ui", webui.Built())
	if !loopback(cfg.Server.Listen) {
		log.Warn("the API has no authentication: anyone who can reach it can start, cancel and read runs", "listen", cfg.Server.Listen)
	}

	// Workers stop before the server, so running steps can still report.
	workersCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()
	kill := make(chan struct{})
	g := supervise.NewGroup(workersCtx, supervise.Options{Kill: kill, Line: func(id, text string, note bool) {
		if note {
			log.Info("worker "+text, "worker", id)
			return
		}
		log.Info(text, "worker", id)
	}})
	groups := 0
	if !noWorkers {
		// Step output is kept by the server (the web UI shows it): the
		// workers' own logs are enough here.
		groups = startWorkers(workersCtx, g, cfg, e.Core, config.ServerURL(e.APIAddr), log, "HUBWAGEN_ECHO_LOGS=0")
	} else if len(cfg.Workers) > 0 {
		log.Info("starting no workers (--no-workers); run them with hubwagen workers", "groups", len(cfg.Workers))
	}
	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		g.Wait()
	}()

	select {
	case <-ctx.Done():
		if groups > 0 {
			log.Info("stopping workers: running steps finish, polls end within their wait; signal again to kill them")
			killOnSignal(kill)
		}
		stopWorkers()
		<-workersDone
		log.Info("stopping")
		if err := e.Close(); err != nil {
			return err
		}
		log.Info("stopped")
		return nil
	case err := <-e.Done():
		close(kill) // the server is gone: nothing for running steps to report to
		stopWorkers()
		<-workersDone
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

// serverConfig is what server.Start needs from the config file: the data
// dir, the listen address and the bucket, with its credentials read.
func serverConfig(cfg *config.Config) (server.Config, error) {
	s3, err := cfg.Bucket.S3()
	if err != nil {
		return server.Config{}, fmt.Errorf("%s: %w", configName(cfg), err)
	}
	return server.Config{DataDir: cfg.DataDir, Listen: cfg.Server.Listen, S3: s3,
		ServeData: cfg.Bucket.Transfers == config.TransfersServer}, nil
}
