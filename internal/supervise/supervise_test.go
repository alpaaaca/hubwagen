package supervise

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// syncBuffer is a bytes.Buffer safe to read while Run writes to it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func sh(id, script string) Process {
	return Process{ID: id, Command: []string{"/bin/sh", "-c", script}, Dir: os.TempDir(), Env: []string{"WHO=" + id}}
}

func TestOutputIsPrefixedAndProcessesRestart(t *testing.T) {
	var out syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, []Process{sh("a-1", `echo "hi from $WHO"; printf partial; exit 3`)},
			Options{Out: &out, MinBackoff: 10 * time.Millisecond})
	}()
	require.Eventually(t, func() bool { return strings.Count(out.String(), "hi from a-1") >= 2 }, 5*time.Second, 10*time.Millisecond)
	cancel()
	<-done
	s := out.String()
	assert.Contains(t, s, "a-1 | ▸ started (pid ")
	assert.Contains(t, s, "a-1 | hi from a-1\n")
	assert.Contains(t, s, "a-1 | partial\n", "a last line without a newline is flushed")
	assert.Contains(t, s, "a-1 | ▸ exited with status 3; restarting in 10ms")
	assert.Contains(t, s, "restarting in 20ms", "backoff doubles")
}

func TestStopSendsSIGTERMThenKills(t *testing.T) {
	var out syncBuffer
	var mu sync.Mutex
	pids := map[string]int{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, []Process{
			sh("polite-1", `trap 'echo bye; exit 0' TERM; echo ready; while :; do sleep 0.05; done`),
			sh("stubborn-1", `trap '' TERM; echo ready; while :; do sleep 0.05; done`),
		}, Options{Out: &out, StopTimeout: 300 * time.Millisecond, OnStart: func(id string, pid int) {
			mu.Lock()
			pids[id] = pid
			mu.Unlock()
		}})
	}()
	require.Eventually(t, func() bool { return strings.Count(out.String(), "| ready") == 2 }, 5*time.Second, 10*time.Millisecond)
	began := time.Now()
	cancel()
	<-done
	assert.Less(t, time.Since(began), 3*time.Second)
	s := out.String()
	assert.Contains(t, s, "polite-1 | bye")
	assert.Contains(t, s, "polite-1 | ▸ stopped")
	assert.Contains(t, s, "stubborn-1 | ▸ stopped")
	mu.Lock()
	defer mu.Unlock()
	for id, pid := range pids {
		assert.Error(t, syscall.Kill(pid, 0), "%s is gone", id)
	}
}

func TestAKilledProcessIsRestarted(t *testing.T) {
	var out syncBuffer
	started := make(chan int, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, []Process{sh("w-1", `while :; do sleep 0.05; done`)},
			Options{Out: &out, MinBackoff: 10 * time.Millisecond, OnStart: func(_ string, pid int) { started <- pid }})
	}()
	require.NoError(t, syscall.Kill(<-started, syscall.SIGKILL))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("not restarted")
	}
	cancel()
	<-done
	assert.Contains(t, out.String(), "w-1 | ▸ ended by signal: killed; restarting in 10ms")
}

func TestKillDoesntWait(t *testing.T) {
	var out syncBuffer
	kill := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, []Process{sh("s-1", `trap '' TERM; echo ready; while :; do sleep 0.05; done`)},
			Options{Out: &out, StopTimeout: time.Minute, Kill: kill})
	}()
	require.Eventually(t, func() bool { return strings.Contains(out.String(), "| ready") }, 5*time.Second, 10*time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)
	began := time.Now()
	close(kill)
	<-done
	assert.Less(t, time.Since(began), 5*time.Second)
}

func TestLineGetsOutputAndNotes(t *testing.T) {
	type line struct {
		id, text string
		note     bool
	}
	var (
		mu    sync.Mutex
		lines []line
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, []Process{sh("a-1", `echo "hi"; sleep 10`)}, Options{Line: func(id, text string, note bool) {
			mu.Lock()
			lines = append(lines, line{id, text, note})
			mu.Unlock()
		}})
	}()
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(lines) >= 2 }, 5*time.Second, 10*time.Millisecond)
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	assert.True(t, lines[0].note)
	assert.True(t, strings.HasPrefix(lines[0].text, "started (pid "), lines[0].text)
	assert.Equal(t, line{"a-1", "hi", false}, lines[1])
	assert.Equal(t, line{"a-1", "stopped", true}, lines[len(lines)-1])
}

func TestGroupStartsAndStopsOneAtATime(t *testing.T) {
	var out syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	g := NewGroup(ctx, Options{Out: &out})
	g.Start(sh("a-1", `trap 'echo bye; exit 0' TERM; echo up; while :; do sleep 0.05; done`))
	g.Start(sh("a-2", `trap 'exit 0' TERM; while :; do sleep 0.05; done`))
	g.Start(sh("a-1", `echo duplicate`)) // already running: ignored
	require.Eventually(t, func() bool { return strings.Contains(out.String(), "a-1 | up") }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, []string{"a-1", "a-2"}, g.Running())

	g.Stop("a-1")
	assert.Equal(t, []string{"a-2"}, g.Running())
	require.Eventually(t, func() bool { return strings.Contains(out.String(), "a-1 | ▸ stopped") }, 5*time.Second, 10*time.Millisecond)
	assert.Contains(t, out.String(), "a-1 | bye", "stopped with SIGTERM")
	assert.NotContains(t, out.String(), "duplicate")
	assert.NotContains(t, out.String(), "restarting", "a stopped process isn't restarted")

	cancel()
	g.Wait()
	assert.Contains(t, out.String(), "a-2 | ▸ stopped")
	assert.Empty(t, g.Running())
}
