package core

import (
	"context"
	"testing"

	"github.com/alper/moksha/internal/clock"
	"github.com/alper/moksha/internal/layout"
	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runCore runs a core on a fresh store until it stops, returning what Run
// returned; batch is one input, applied in the loop.
func runCore(t *testing.T, batch func(c *Core, st *state.Store, cancel context.CancelFunc)) (submitErr, runErr error) {
	t.Helper()
	st, err := state.Open(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	term, err := st.StartTerm(context.Background(), 0)
	require.NoError(t, err)
	c := New(Config{Store: st, Term: term, Clock: clock.NewFake(t0), Layout: layout.New("file:///bucket")})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	_, submitErr = submit(context.Background(), c, func(c *Core) (struct{}, error) {
		c.b.events = append(c.b.events, state.Event{Time: t0, Type: "test"})
		batch(c, st, cancel)
		return struct{}{}, nil
	})
	cancel()
	return submitErr, <-done
}

// Ctrl-C while a batch commits cancels the commit: a stop, not a failure,
// so moksha dev and moksha server exit cleanly.
func TestACommitCancelledByTheStopIsAStop(t *testing.T) {
	submitErr, runErr := runCore(t, func(_ *Core, _ *state.Store, cancel context.CancelFunc) { cancel() })
	assert.ErrorIs(t, submitErr, context.Canceled, "the batch wasn't recorded: its caller hears so")
	assert.NoError(t, runErr)
}

func TestACommitThatFailsStopsTheCoreWithItsError(t *testing.T) {
	submitErr, runErr := runCore(t, func(_ *Core, st *state.Store, _ context.CancelFunc) {
		_, err := st.StartTerm(context.Background(), 0) // another leader took over
		require.NoError(t, err)
	})
	assert.ErrorIs(t, submitErr, state.ErrStaleTerm)
	assert.ErrorIs(t, runErr, state.ErrStaleTerm)
}
