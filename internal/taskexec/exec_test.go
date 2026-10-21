package taskexec

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/spec"
	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

type fixture struct {
	t      *testing.T
	bucket blob.Bucket
	layout layout.Layout
	opts   Options
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	b, err := blob.Open(blob.FileURI(t.TempDir()))
	require.NoError(t, err)
	return &fixture{t: t, bucket: b, layout: layout.New(b.Base()), opts: Options{
		Bucket: b, BaseEnv: []string{"PATH=" + os.Getenv("PATH"), "INHERITED=yes", "HUBWAGEN_SECRET_T_DB_PASSWORD=s3cret"},
		TerminationGrace: 200 * time.Millisecond, FlushInterval: 50 * time.Millisecond,
	}}
}

// assignment builds an attempt of task "a" running `sh -c script`, with the
// given outputs.
func (f *fixture) assignment(attempt int, script string, outputs ...string) core.Assignment {
	runKey := "t/p/20261020T000000Z_20261021T000000Z"
	asg := core.Assignment{
		AttemptID: "att-" + string(rune('0'+attempt)), RunID: "run1", TaskID: "a", Attempt: attempt, Term: 1,
		Tenant: "t", Pipeline: "p", RunKey: runKey, Generation: 1,
		Interval: &spec.Interval{Start: time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)},
		Command:  []string{"/bin/sh", "-c"}, Args: []string{script},
		Timeout: 10 * time.Second, Inputs: map[string]string{}, Outputs: map[string]core.OutputTarget{},
		LogsURI:        f.layout.Logs("t", "p", "run1", "a", attempt),
		ResultURI:      f.layout.Result("t", "run1", "a", attempt),
		CommitURI:      f.layout.TaskCommit("t", "p", "a", runKey, 1),
		IdempotencyKey: runKey + "/gen-1/a",
	}
	for _, o := range outputs {
		gen := f.layout.OutputGeneration("t", "p", "a", o, runKey, 1)
		asg.Outputs[o] = core.OutputTarget{Staging: layout.Staging(gen, attempt), Generation: gen}
	}
	return asg
}

// logs reads every shipped log line of an attempt.
func (f *fixture) logs(asg core.Assignment) []logLine {
	f.t.Helper()
	uris, err := f.bucket.List(ctx, asg.LogsURI)
	require.NoError(f.t, err)
	var out []logLine
	for _, u := range uris {
		if strings.HasSuffix(u, "index.json") {
			continue
		}
		data, err := f.bucket.Get(ctx, u)
		require.NoError(f.t, err)
		sc := bufio.NewScanner(bytes.NewReader(data))
		sc.Buffer(nil, 1<<20)
		for sc.Scan() {
			var l logLine
			require.NoError(f.t, json.Unmarshal(sc.Bytes(), &l))
			out = append(out, l)
		}
	}
	return out
}

func (f *fixture) index(asg core.Assignment) logIndex {
	f.t.Helper()
	data, err := f.bucket.Get(ctx, asg.LogsURI+"index.json")
	require.NoError(f.t, err)
	var idx logIndex
	require.NoError(f.t, json.Unmarshal(data, &idx))
	return idx
}

func lines(ls []logLine, stream string) []string {
	var out []string
	for _, l := range ls {
		if stream == "" || l.Stream == stream {
			out = append(out, l.Line)
		}
	}
	return out
}

func TestSuccessCommitsOutputsAndShipsLogs(t *testing.T) {
	f := newFixture(t)
	asg := f.assignment(1, `
echo "extracting"
echo "warning: slow" >&2
printf 'id,amount\n1,10\n' > "$HUBWAGEN_OUTPUT_ROWS_PATH/rows.csv"
echo '{"outputs": {"rows": {"rows": 1}}, "metadata": {"source": "test"}}' > "$HUBWAGEN_RESULT_FILE"
`, "rows")

	rep := Run(ctx, asg, f.opts)
	require.Equal(t, state.AttemptSucceeded, rep.Status, rep.Message)
	assert.Equal(t, 0, *rep.ExitCode)
	require.Len(t, rep.Outputs, 1)
	assert.Equal(t, asg.Outputs["rows"].Staging, rep.Outputs[0].URI)
	assert.Equal(t, float64(1), rep.Outputs[0].Metadata["rows"])
	assert.JSONEq(t, `{"outputs": {"rows": {"rows": 1}}, "metadata": {"source": "test"}}`, string(rep.Result))

	data, err := f.bucket.Get(ctx, asg.Outputs["rows"].Staging+"rows.csv")
	require.NoError(t, err)
	assert.Equal(t, "id,amount\n1,10\n", string(data))

	m, err := ReadMarker(ctx, f.bucket, asg.CommitURI)
	require.NoError(t, err)
	assert.Equal(t, asg.AttemptID, m.AttemptID)
	_, err = f.bucket.Get(ctx, layout.CommitMarker(asg.Outputs["rows"].Generation))
	assert.NoError(t, err, "per-output _COMMITTED written too")
	_, err = f.bucket.Get(ctx, asg.ResultURI)
	assert.NoError(t, err)

	ls := f.logs(asg)
	assert.Equal(t, []string{"extracting"}, lines(ls, "stdout"))
	assert.Equal(t, []string{"warning: slow"}, lines(ls, "stderr"))
	idx := f.index(asg)
	assert.Equal(t, state.AttemptSucceeded, idx.Status)
	assert.Equal(t, 2, idx.Lines)
}

func TestEnvironmentContract(t *testing.T) {
	f := newFixture(t)
	asg := f.assignment(2, `env | sort`, "out-put")
	asg.Inputs["orders"] = f.bucket.Base() + "/data/t/p/x/orders/k/gen-1/attempt-1/"
	asg.Env = map[string]string{"MODE": "full"}
	asg.Secrets = []spec.Secret{{Name: "db", Key: "password", Env: "DB_PASSWORD"}}
	f.opts.Secrets = EnvSecrets{Environ: f.opts.BaseEnv}

	rep := Run(ctx, asg, f.opts)
	require.Equal(t, state.AttemptSucceeded, rep.Status, rep.Message)
	env := map[string]string{}
	for _, l := range lines(f.logs(asg), "stdout") {
		k, v, _ := strings.Cut(l, "=")
		env[k] = v
	}

	for k, want := range map[string]string{
		"HUBWAGEN_TENANT": "t", "HUBWAGEN_PIPELINE": "p", "HUBWAGEN_RUN_ID": "run1",
		"HUBWAGEN_RUN_KEY": asg.RunKey, "HUBWAGEN_RUN_GENERATION": "1", "HUBWAGEN_TASK_ID": "a",
		"HUBWAGEN_ATTEMPT": "2", "HUBWAGEN_ATTEMPT_ID": asg.AttemptID, "HUBWAGEN_IDEMPOTENCY_KEY": asg.IdempotencyKey,
		"HUBWAGEN_INTERVAL_START": "2026-10-20T00:00:00Z", "HUBWAGEN_INTERVAL_END": "2026-10-21T00:00:00Z",
		"HUBWAGEN_INPUT_ORDERS": asg.Inputs["orders"], "HUBWAGEN_OUTPUT_OUT_PUT": asg.Outputs["out-put"].Staging,
		"MODE": "full", "DB_PASSWORD": "s3cret", "INHERITED": "yes",
	} {
		assert.Equal(t, want, env[k], k)
	}
	p, _ := f.bucket.LocalPath(asg.Outputs["out-put"].Staging)
	assert.Equal(t, p, env["HUBWAGEN_OUTPUT_OUT_PUT_PATH"])
	assert.DirExists(t, p)
	assert.NotContains(t, env, "HUBWAGEN_SECRET_T_DB_PASSWORD", "secret source variables are not passed on")
	// macOS reports /var/... as /private/var/..., hence the suffix match.
	assert.True(t, strings.HasSuffix(env["PWD"], env["HUBWAGEN_WORKDIR"]), "runs in its scratch directory: %s", env["PWD"])
	assert.NoDirExists(t, env["HUBWAGEN_WORKDIR"], "scratch directory removed afterwards")
}

func TestMissingSecretFails(t *testing.T) {
	f := newFixture(t)
	asg := f.assignment(1, "true")
	asg.Secrets = []spec.Secret{{Name: "api", Key: "token", Env: "TOKEN"}}
	f.opts.Secrets = EnvSecrets{}
	rep := Run(ctx, asg, f.opts)
	assert.Equal(t, "secret_error", rep.Reason)
	assert.Contains(t, rep.Message, "HUBWAGEN_SECRET_T_API_TOKEN")
}

func TestFailureClassification(t *testing.T) {
	f := newFixture(t)
	cases := []struct {
		name    string
		command []string
		reason  string
		exit    *int
	}{
		{"non-zero exit", []string{"/bin/sh", "-c", "exit 3"}, "exit_code", ptr(3)},
		{"not found via shell", []string{"/bin/sh", "-c", "no-such-command-xyz"}, "exec_error", ptr(127)},
		{"not found directly", []string{"/no/such/binary"}, "exec_error", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			asg := f.assignment(1, "")
			asg.AttemptID += tc.name
			asg.LogsURI = f.layout.Logs("t", "p", "run1", "a", len(tc.name))
			asg.Command, asg.Args = tc.command, nil
			rep := Run(ctx, asg, f.opts)
			assert.Equal(t, state.AttemptFailed, rep.Status)
			assert.Equal(t, tc.reason, rep.Reason)
			assert.Equal(t, tc.exit, rep.ExitCode)
			_, err := f.bucket.Get(ctx, asg.CommitURI)
			assert.ErrorIs(t, err, blob.ErrNotExist, "failures never commit")
		})
	}
}

func TestTimeoutKillsTheWholeProcessGroup(t *testing.T) {
	f := newFixture(t)
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	// The shell ignores TERM; its background child must still die.
	asg := f.assignment(1, `trap '' TERM; sleep 30 & echo $! > `+pidFile+`; wait`)
	asg.Timeout = 300 * time.Millisecond

	start := time.Now()
	rep := Run(ctx, asg, f.opts)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Equal(t, state.AttemptFailed, rep.Status)
	assert.Equal(t, "timeout", rep.Reason)
	assert.Nil(t, rep.ExitCode)

	pid, err := os.ReadFile(pidFile)
	require.NoError(t, err)
	assert.Eventually(t, func() bool { return !processAlive(strings.TrimSpace(string(pid))) }, 2*time.Second, 50*time.Millisecond)
	assert.Contains(t, strings.Join(lines(f.logs(asg), "hubwagen"), "\n"), "timeout: sent SIGTERM")
}

func TestCancellation(t *testing.T) {
	f := newFixture(t)
	asg := f.assignment(1, `echo started; sleep 30`)
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan core.Report)
	go func() { done <- Run(cctx, asg, f.opts) }()
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case rep := <-done:
		assert.Equal(t, state.AttemptCancelled, rep.Status)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	assert.Equal(t, []string{"started"}, lines(f.logs(asg), "stdout"))
}

func TestSecondAttemptIsSuperseded(t *testing.T) {
	f := newFixture(t)
	first := f.assignment(1, `echo one > "$HUBWAGEN_OUTPUT_O_PATH/x"`, "o")
	second := f.assignment(2, `echo two > "$HUBWAGEN_OUTPUT_O_PATH/x"`, "o")

	require.Equal(t, state.AttemptSucceeded, Run(ctx, first, f.opts).Status)
	rep := Run(ctx, second, f.opts)
	assert.Equal(t, state.AttemptSuperseded, rep.Status)
	assert.Equal(t, first.AttemptID, rep.Winner)
	require.Len(t, rep.Outputs, 1)
	assert.Equal(t, first.Outputs["o"].Staging, rep.Outputs[0].URI, "downstream reads the winner's data")
}

func TestInvalidResultFile(t *testing.T) {
	f := newFixture(t)
	for name, content := range map[string]string{
		"not json":      `oops`,
		"unknown field": `{"rows": 1}`,
		"undeclared":    `{"outputs": {"other": {}}}`,
		"too large":     `{"metadata": {"x": "` + strings.Repeat("a", maxResultBytes) + `"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			src := filepath.Join(t.TempDir(), "result.json")
			require.NoError(t, os.WriteFile(src, []byte(content), 0o644))
			asg := f.assignment(1, `cp `+src+` "$HUBWAGEN_RESULT_FILE"`, "o")
			asg.CommitURI += name
			rep := Run(ctx, asg, f.opts)
			assert.Equal(t, "result_error", rep.Reason, rep.Message)
			_, err := f.bucket.Get(ctx, asg.CommitURI)
			assert.ErrorIs(t, err, blob.ErrNotExist)
		})
	}
}

func TestLogChunkingAndLongLines(t *testing.T) {
	f := newFixture(t)
	f.opts.ChunkBytes = 1024
	asg := f.assignment(1, `i=0; while [ $i -lt 200 ]; do echo "line $i"; i=$((i+1)); done; head -c 100000 /dev/zero | tr '\0' x; echo`)
	rep := Run(ctx, asg, f.opts)
	require.Equal(t, state.AttemptSucceeded, rep.Status)

	got := lines(f.logs(asg), "stdout")
	require.GreaterOrEqual(t, len(got), 202)
	assert.Equal(t, "line 0", got[0])
	assert.Equal(t, "line 199", got[199])
	assert.Equal(t, maxLineBytes, len(got[200]), "long lines are split, not dropped")
	assert.Greater(t, f.index(asg).Chunks, 5)
}

func TestEcho(t *testing.T) {
	f := newFixture(t)
	var buf bytes.Buffer
	f.opts.Echo = &buf
	asg := f.assignment(1, `echo hi`)
	Run(ctx, asg, f.opts)
	assert.Equal(t, "[a] hi\n", buf.String())
}

func TestRecovery(t *testing.T) {
	f := newFixture(t)
	won := f.assignment(1, `echo > "$HUBWAGEN_OUTPUT_O_PATH/x"`, "o")
	require.Equal(t, state.AttemptSucceeded, Run(ctx, won, f.opts).Status)

	reps := Recovery{Bucket: f.bucket}.Recover(ctx, []core.InFlight{
		{Attempt: state.Attempt{ID: won.AttemptID}, CommitURI: won.CommitURI},
		{Attempt: state.Attempt{ID: "att-2"}, CommitURI: won.CommitURI},
		{Attempt: state.Attempt{ID: "att-9"}, CommitURI: f.layout.TaskCommit("t", "p", "other", "k", 1)},
	})
	require.Len(t, reps, 2, "no marker, no report: the core records it as lost")
	assert.Equal(t, state.AttemptSucceeded, reps[0].Status)
	assert.Equal(t, won.Outputs["o"].Staging, reps[0].Outputs[0].URI)
	assert.Equal(t, state.AttemptSuperseded, reps[1].Status)
	assert.Equal(t, won.AttemptID, reps[1].Winner)
}

func ptr[T any](v T) *T { return &v }

func processAlive(pid string) bool {
	_, err := os.Stat("/proc/" + pid)
	if err == nil {
		return true
	}
	// No /proc (macOS): ask kill -0 through the shell.
	out, _ := runQuiet("/bin/sh", "-c", "kill -0 "+pid+" 2>/dev/null && echo alive")
	return strings.TrimSpace(out) == "alive"
}
