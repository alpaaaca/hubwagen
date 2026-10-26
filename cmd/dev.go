package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/alper/hubwagen/internal/devmode"
)

var (
	dataDir   string
	devListen string
)

var devCmd = &cobra.Command{
	Use:   "dev",
	Short: "Run a hubwagen server on this machine",
	Long: `Dev runs the whole server in this process, with its state, logbooks and
pallets in --data-dir, and serves the API on --listen. Workers connect to it
(python -m hubwagen.worker app.hauls) and run the hauls that
hubwagen start and hubwagen replay start.

It prints a line for every notable event: hauls started, stuck and
finished, lifts scheduled (and why), reused, retried and finished, builds
registered. Ctrl-C stops it; work that workers are running carries on and
is picked up on the next start.

Only one process can use a data directory at a time.`,
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE:         runDev,
}

func init() {
	devCmd.Flags().StringVar(&dataDir, "data-dir", ".hubwagen", "directory for the server's state, logbooks and pallets")
	devCmd.Flags().StringVar(&devListen, "listen", "127.0.0.1:7070", "address to serve the API on")
	rootCmd.AddCommand(devCmd)
}

func runDev(cmd *cobra.Command, _ []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	e, err := devmode.Start(ctx, devmode.Config{DataDir: dataDir, Listen: devListen})
	if err != nil {
		return err
	}
	defer e.Close()
	from := e.LastSeq(ctx)
	fmt.Printf("hubwagen dev: data in %s; Ctrl-C to stop\n", e.DataDir)
	fmt.Printf("hubwagen dev: API on http://%s\n", e.APIAddr)
	go e.Follow(ctx, from, func(line string) { fmt.Println("·", line) })

	select {
	case <-ctx.Done():
		fmt.Println("stopping")
		return e.Close()
	case err := <-e.Done():
		return fmt.Errorf("server stopped: %w", err)
	}
}
