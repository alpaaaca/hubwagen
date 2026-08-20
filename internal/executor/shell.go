package executor

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/alper/hubwagen/internal/dag"
)

type ShellExecutor struct{}

func (s *ShellExecutor) Type() string { return "shell" }

func (s *ShellExecutor) Run(ctx context.Context, step dag.Step, logWriter io.Writer) (StepResult, error) {
	cmd := exec.CommandContext(ctx, "/bin/bash", "-c", step.Command)

	// Run bash in its own process group so cancellation can signal the whole
	// group, not just bash. Signalling bash alone is not enough: for a
	// multi-command script bash forks real children and defers acting on a
	// signal until control returns to it between commands, so e.g. a `sleep`
	// step would run to completion before bash ever notices SIGTERM. Signalling
	// the group reaches the actual running child directly.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// SIGTERM on context cancel; WaitDelay gives 5s before SIGKILL
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
	cmd.WaitDelay = 5 * time.Second

	env := os.Environ()
	for k, v := range step.Env {
		env = append(env, k+"="+v)
	}
	cmd.Env = env

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return StepResult{ExitCode: 1}, fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return StepResult{ExitCode: 1}, fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return StepResult{ExitCode: 1}, fmt.Errorf("starting command: %w", err)
	}

	var wg sync.WaitGroup
	scan := func(r io.Reader) {
		defer wg.Done()
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			fmt.Fprintln(logWriter, sc.Text())
		}
	}
	wg.Add(2)
	go scan(stdout)
	go scan(stderr)
	wg.Wait()

	_ = cmd.Wait()

	exitCode := 0
	if cmd.ProcessState != nil {
		exitCode = cmd.ProcessState.ExitCode()
		if exitCode < 0 {
			exitCode = 1
		}
	}

	return StepResult{ExitCode: exitCode}, nil
}
