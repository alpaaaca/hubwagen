// Package devmode is what `hubwagen dev` adds to the server (package
// server): it prints runs and what their lifts print (Console), or the
// engine's events (Follow, Describe).
package devmode

import (
	"context"

	"github.com/alper/hubwagen/internal/server"
)

// Engine is a running server that dev prints from.
type Engine struct{ *server.Engine }

// Config is the server's config.
type Config = server.Config

// Start starts the server; see server.Start.
func Start(ctx context.Context, cfg Config) (*Engine, error) {
	s, err := server.Start(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Engine{s}, nil
}
