// Package taskexec runs one attempt under the task contract
// (docs/task-contract.md): it prepares the environment and directories, runs
// the command in its own process group, ships logs to the bucket, enforces
// the timeout, reads the result file, and commits outputs with a
// create-if-absent marker.
//
// The same code path serves every runtime: the dev-mode local runtime calls
// Run in-process; container launchers and Kubernetes Jobs will run it as
// `hubwagen exec`.
package taskexec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/state"
)

type Options struct {
	Bucket blob.Bucket
	// Secrets resolves the task's secret references. Nil: tasks with
	// secrets fail.
	Secrets SecretProvider
	// BaseEnv is the environment host processes inherit (default:
	// os.Environ()). HUBWAGEN_* variables are always removed from it.
	BaseEnv []string
	// Echo, if set, also receives every log line, prefixed by the task ID.
	Echo io.Writer
	// TerminationGrace is how long a process gets between SIGTERM and
	// SIGKILL (default 30s).
	TerminationGrace time.Duration
	FlushInterval    time.Duration // log chunk interval (default 2s)
	ChunkBytes       int           // log chunk size (default 1 MiB)
}

const maxResultBytes = 64 << 10

// Run executes the attempt and reports the outcome. It does not report the
// attempt as started; the caller does that before calling Run.
func Run(ctx context.Context, asg core.Assignment, o Options) core.Report {
	if o.TerminationGrace == 0 {
		o.TerminationGrace = 30 * time.Second
	}
	if o.BaseEnv == nil {
		o.BaseEnv = os.Environ()
	}
	rep := core.Report{AttemptID: asg.AttemptID}
	fail := func(reason, format string, args ...any) core.Report {
		rep.Status = state.AttemptFailed
		rep.Reason = reason
		rep.Message = fmt.Sprintf(format, args...)
		return rep
	}

	workdir, err := os.MkdirTemp("", "hubwagen-"+asg.AttemptID+"-")
	if err != nil {
		return fail("setup_error", "creating workdir: %v", err)
	}
	defer os.RemoveAll(workdir)
	for name, target := range asg.Outputs {
		if p, ok := o.Bucket.LocalPath(target.Staging); ok {
			if err := os.MkdirAll(p, 0o755); err != nil {
				return fail("setup_error", "creating staging directory for %s: %v", name, err)
			}
		}
	}

	resultFile := filepath.Join(workdir, ".hubwagen-result.json")
	env, err := buildEnv(ctx, asg, o, workdir, resultFile)
	if err != nil {
		return fail("secret_error", "%v", err)
	}

	logs := newLogShipper(o.Bucket, asg.LogsURI, asg.TaskID, o.Echo, o.FlushInterval, o.ChunkBytes)
	exitCode, how, startErr := runProcess(ctx, asg, env, workdir, logs, o.TerminationGrace)

	switch {
	case startErr != nil:
		logs.system("cannot start command: %v", startErr)
		rep = fail("exec_error", "cannot start %q: %v", asg.Command[0], startErr)
	case how == "timeout":
		rep = fail("timeout", "killed after %s", asg.Timeout)
	case how == "cancelled":
		rep.Status, rep.Reason, rep.Message = state.AttemptCancelled, "cancelled", "cancelled by the scheduler"
	case exitCode == 126 || exitCode == 127:
		rep = fail("exec_error", "command not executable or not found (exit %d)", exitCode)
	case exitCode != 0:
		rep = fail("exit_code", "exited with %d", exitCode)
	}
	if startErr == nil && how == "" {
		rep.ExitCode = &exitCode
	}
	if rep.Status == "" {
		rep = commit(ctx, asg, o, resultFile, rep)
	}
	logs.close(rep.Status)
	return rep
}

// runProcess runs the command in its own process group and returns its exit
// code and, if it was stopped, why ("timeout" or "cancelled").
func runProcess(ctx context.Context, asg core.Assignment, env []string, workdir string, logs *logShipper, grace time.Duration) (int, string, error) {
	argv := append(append([]string{}, asg.Command...), asg.Args...)
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = workdir
	cmd.Env = env
	// Its own process group, so stopping reaches every child, not just the
	// top process (a shell would otherwise defer the signal until its
	// current child exits).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, "", err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return 0, "", err
	}
	if err := cmd.Start(); err != nil {
		return 0, "", err
	}

	streamsDone := make(chan struct{}, 2)
	go func() { logs.read("stdout", stdout); streamsDone <- struct{}{} }()
	go func() { logs.read("stderr", stderr); streamsDone <- struct{}{} }()
	waited := make(chan error, 1)
	go func() {
		<-streamsDone
		<-streamsDone
		waited <- cmd.Wait()
	}()

	var timeout <-chan time.Time
	if asg.Timeout > 0 {
		t := time.NewTimer(asg.Timeout)
		defer t.Stop()
		timeout = t.C
	}

	how := ""
	select {
	case <-waited:
		return cmd.ProcessState.ExitCode(), "", nil
	case <-ctx.Done():
		how = "cancelled"
	case <-timeout:
		how = "timeout"
	}

	pgid := -cmd.Process.Pid
	_ = syscall.Kill(pgid, syscall.SIGTERM)
	logs.system("%s: sent SIGTERM, SIGKILL in %s", how, grace)
	select {
	case <-waited:
	case <-time.After(grace):
		_ = syscall.Kill(pgid, syscall.SIGKILL)
		// Wait for the pipes to drain; give up if a stray process keeps
		// them open after the kill.
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
		}
	}
	code := -1
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
	}
	return code, how, nil
}

// buildEnv assembles the contract environment on top of the inherited one.
func buildEnv(ctx context.Context, asg core.Assignment, o Options, workdir, resultFile string) ([]string, error) {
	vars := map[string]string{
		"HUBWAGEN_TENANT":          asg.Tenant,
		"HUBWAGEN_PIPELINE":        asg.Pipeline,
		"HUBWAGEN_RUN_ID":          asg.RunID,
		"HUBWAGEN_RUN_KEY":         asg.RunKey,
		"HUBWAGEN_RUN_GENERATION":  fmt.Sprint(asg.Generation),
		"HUBWAGEN_TASK_ID":         asg.TaskID,
		"HUBWAGEN_ATTEMPT":         fmt.Sprint(asg.Attempt),
		"HUBWAGEN_ATTEMPT_ID":      asg.AttemptID,
		"HUBWAGEN_IDEMPOTENCY_KEY": asg.IdempotencyKey,
		"HUBWAGEN_RESULT_FILE":     resultFile,
		"HUBWAGEN_WORKDIR":         workdir,
		"HUBWAGEN_INTERVAL_START":  "",
		"HUBWAGEN_INTERVAL_END":    "",
	}
	if asg.Interval != nil {
		vars["HUBWAGEN_INTERVAL_START"] = asg.Interval.Start.UTC().Format(time.RFC3339)
		vars["HUBWAGEN_INTERVAL_END"] = asg.Interval.End.UTC().Format(time.RFC3339)
	}
	for name, uri := range asg.Inputs {
		setURI(vars, o.Bucket, "HUBWAGEN_INPUT_"+envSuffix(name), uri)
	}
	for name, target := range asg.Outputs {
		setURI(vars, o.Bucket, "HUBWAGEN_OUTPUT_"+envSuffix(name), target.Staging)
	}
	for k, v := range asg.Env {
		vars[k] = v
	}
	if len(asg.Secrets) > 0 {
		if o.Secrets == nil {
			return nil, errors.New("task uses secrets but no secret provider is configured")
		}
		resolved, err := o.Secrets.Resolve(ctx, asg.Tenant, asg.Secrets)
		if err != nil {
			return nil, err
		}
		for k, v := range resolved {
			vars[k] = v
		}
	}

	env := make([]string, 0, len(o.BaseEnv)+len(vars))
	for _, kv := range o.BaseEnv {
		if !hasPrefixFold(kv, "HUBWAGEN_") {
			env = append(env, kv)
		}
	}
	for k, v := range vars {
		env = append(env, k+"="+v)
	}
	return env, nil
}

// setURI sets name to the URI and, for local buckets, name_PATH to the plain
// filesystem path, so scripts don't have to parse file:// URIs.
func setURI(vars map[string]string, b blob.Bucket, name, uri string) {
	vars[name] = uri
	if p, ok := b.LocalPath(uri); ok {
		vars[name+"_PATH"] = p
	}
}
