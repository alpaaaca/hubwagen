package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/alper/moksha/internal/dataversion"
	"github.com/alper/moksha/internal/state"
)

// Memoization. A step that opts in (Command.Memo) gets a fingerprint: a hash
// of its name, its code version in the build that replayed the run, its
// input data versions' content, its arguments and its output, if any. If what an
// earlier run with that fingerprint produced is still there (and young
// enough, for a TTL), the step is not run: the run continues with it. For a
// step with an output that is a committed data version; for one without, the
// result of a step that ran and completed, which the store always has.
// Otherwise the step runs, and its scheduling records why (RunReason).
//
// Arguments are part of the fingerprint as canonical JSON (keys sorted), so
// the SDK passes data versions as Inputs, not inside Args: then an identical
// rebuild upstream (same content hash) doesn't change the fingerprint.

// BuildRequest registers a worker build: the pipelines it defines and the code
// version of each step.
type BuildRequest struct {
	ID            string
	Pipelines     []string
	Steps         map[string]string        // step name → code version
	Schedules     []ScheduleSpec           // for pipelines it defines; see schedules.go
	Subscriptions []SubscriptionSpec       // for pipelines it defines; see subscriptions.go
	Datasets      []DatasetSpec            // retention of datasets its steps write; see retention.go
	Info          map[string]state.RunInfo // titles and descriptions of pipelines it defines
}

// RegisterBuild records a build. Registering the same build again is a
// no-op; reusing a build ID with other runs or versions is an error, since
// builds are immutable (a build recorded before runs were, gets its runs
// added). A new build gives stuck runs a replay: it may carry the fix.
//
// Workers that poll with a build ID get only the run tasks and steps their
// build defines, so projects with different code can share queues.
func (c *Core) RegisterBuild(ctx context.Context, req BuildRequest) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) { return struct{}{}, c.registerBuild(req) })
	return err
}

// registerBuild records a build in the current batch (RegisterBuild,
// Deploy).
func (c *Core) registerBuild(req BuildRequest) error {
	if req.ID == "" {
		return fmt.Errorf("%w: a build needs an ID", ErrInvalid)
	}
	runs := slices.Compact(slices.Sorted(slices.Values(req.Pipelines)))
	if slices.Contains(runs, "") {
		return fmt.Errorf("%w: build %s: pipeline names must not be empty", ErrInvalid, req.ID)
	}
	if old, ok := c.builds[req.ID]; ok {
		if !mapsEqual(old, req.Steps) {
			return fmt.Errorf("%w: build %s is already registered with other code versions", ErrInvalid, req.ID)
		}
		known := c.buildPipelines[req.ID]
		switch {
		case len(known) == 0 && len(runs) > 0: // recorded before runs were
		case !setEqual(known, runs):
			return fmt.Errorf("%w: build %s is already registered with other pipelines", ErrInvalid, req.ID)
		default:
			return nil
		}
	}
	schedules, err := validateSchedules(runs, req.Schedules)
	if err != nil {
		return fmt.Errorf("build %s: %w", req.ID, err)
	}
	subs, err := validateSubscriptions(runs, req.Subscriptions)
	if err != nil {
		return fmt.Errorf("build %s: %w", req.ID, err)
	}
	if err := validateDatasets(req.Datasets); err != nil {
		return fmt.Errorf("build %s: %w", req.ID, err)
	}
	for name := range req.Info {
		if !slices.Contains(runs, name) {
			return fmt.Errorf("%w: build %s: info for pipeline %s, which it does not define", ErrInvalid, req.ID, name)
		}
	}
	steps := map[string]string{}
	for k, v := range req.Steps {
		if k == "" || v == "" {
			return fmt.Errorf("%w: build %s: step names and code versions must not be empty", ErrInvalid, req.ID)
		}
		steps[k] = v
	}
	c.addBuild(req.ID, steps, runs)
	info := map[string]state.RunInfo{}
	for name, i := range req.Info {
		if i != (state.RunInfo{}) {
			info[name] = i
		}
	}
	b := state.Build{ID: req.ID, Pipelines: runs, Info: info, Steps: steps, RegisteredAt: c.now()}
	c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutBuild(b) })
	c.emit(state.EventBuildRegistered, "", buildRegisteredPayload{ID: req.ID, Pipelines: runs, Info: info, Steps: steps})
	c.applySchedules(req.ID, runs, schedules)
	c.applySubscriptions(req.ID, runs, subs)
	c.applyRetention(req.ID, req.Datasets)
	c.resumeStuckRuns()
	return nil
}

func (c *Core) addBuild(id string, steps map[string]string, runs []string) {
	c.builds[id] = steps
	set := map[string]bool{}
	for _, h := range runs {
		set[h] = true
		c.pipelineNames[h] = true
	}
	c.buildPipelines[id] = set
}

// canRun reports whether a worker of build can take t: its build defines the
// run or step. Pollers without a build ID, and builds recorded without
// runs, take any run task.
func (c *Core) canRun(build string, t Task) bool {
	if build == "" {
		return true
	}
	if t.Kind == TaskRun {
		runs := c.buildPipelines[build]
		return len(runs) == 0 || runs[t.Name]
	}
	_, ok := c.builds[build][t.Name]
	return ok
}

func setEqual(set map[string]bool, list []string) bool {
	if len(set) != len(list) {
		return false
	}
	for _, v := range list {
		if !set[v] {
			return false
		}
	}
	return true
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// memoPolicy is a parsed Command.Memo.
type memoPolicy struct {
	on  bool
	ttl time.Duration // 0 = any age
}

// parseMemo reads "" or "never", "fingerprint", "ttl:<duration>".
func parseMemo(s string) (memoPolicy, error) {
	switch {
	case s == "" || s == "never":
		return memoPolicy{}, nil
	case s == "fingerprint":
		return memoPolicy{on: true}, nil
	case strings.HasPrefix(s, "ttl:"):
		d, err := time.ParseDuration(strings.TrimPrefix(s, "ttl:"))
		if err != nil || d <= 0 {
			return memoPolicy{}, fmt.Errorf("memo %q: want ttl:<positive duration>, e.g. ttl:1h", s)
		}
		return memoPolicy{on: true, ttl: d}, nil
	}
	return memoPolicy{}, fmt.Errorf("memo %q is not never, fingerprint or ttl:<duration>", s)
}

// fingerprintInputs is everything a fingerprint covers. It is stored on the
// step, so a later run can say what changed.
type fingerprintInputs struct {
	Step        string          `json:"step"`
	CodeVersion string          `json:"code_version"`
	Inputs      []inputKey      `json:"inputs,omitempty"`
	Args        json.RawMessage `json:"args,omitempty"` // canonical
	Dataset     string          `json:"dataset"`
	Partition   string          `json:"partition,omitempty"`
}

type inputKey struct {
	Name string `json:"name,omitempty"`
	Key  string `json:"key"` // content hash, else data version ID
}

func (f fingerprintInputs) hash() string {
	b, err := json.Marshal(f)
	if err != nil {
		panic(err) // plain struct of strings and canonical JSON
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// canonicalJSON re-encodes a JSON value with object keys sorted and numbers
// kept exactly as written.
func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return json.Marshal(v) // maps marshal with sorted keys
}

// errUnreadable: a run with the fingerprint exists, but what it built can't
// be read any more, so it isn't reused.
var errUnreadable = errors.New("can't be read")

// memoMatch is what a reused step completes with: the data version (if it has an
// output) and result of the step that ran with the same fingerprint.
type memoMatch struct {
	dataVersion string
	runID       string
	seq         int
	result      json.RawMessage
}

// memoize computes l's fingerprint and looks for something to reuse. It sets
// the step's fingerprint and, when the step has to run, its RunReason.
func (c *Core) memoize(l *stepState, inputs []state.DataVersion) *memoMatch {
	h := l.run
	if !l.memo.on {
		l.rec.RunReason = "not memoized"
		return nil
	}
	version, ok := c.builds[h.rec.BuildID][l.rec.Name]
	if !ok {
		l.rec.RunReason = "unknown code version"
		return nil
	}
	args, err := canonicalJSON(l.rec.Args)
	if err != nil {
		l.rec.RunReason = "arguments are not JSON"
		return nil
	}
	fpi := fingerprintInputs{Step: l.rec.Name, CodeVersion: version, Args: args}
	if l.output != nil {
		fpi.Dataset, fpi.Partition = l.output.Dataset, l.output.Partition
	}
	for i, in := range l.inputs {
		key := inputs[i].ContentHash
		if key == "" {
			key = inputs[i].ID
		}
		fpi.Inputs = append(fpi.Inputs, inputKey{Name: in.Name, Key: key})
	}
	sort.Slice(fpi.Inputs, func(i, j int) bool {
		if fpi.Inputs[i].Name != fpi.Inputs[j].Name {
			return fpi.Inputs[i].Name < fpi.Inputs[j].Name
		}
		return fpi.Inputs[i].Key < fpi.Inputs[j].Key
	})
	l.rec.Fingerprint = fpi.hash()
	l.rec.FPInputs = marshalPayload(state.EventStepScheduled, fpi)

	var since time.Time
	if l.memo.ttl > 0 {
		since = c.now().Add(-l.memo.ttl)
	}
	m, err := c.lookupMemo(h.rec.Tenant, l, since)
	switch {
	case err == nil:
		return &m
	case errors.Is(err, errUnreadable):
		l.rec.RunReason = fmt.Sprintf("the earlier run's output %v", err)
		return nil
	case !errors.Is(err, state.ErrNotFound):
		c.log.Warn("memo lookup failed", "run", h.rec.ID, "seq", l.rec.Seq, "err", err)
		l.rec.RunReason = "memo lookup failed"
		return nil
	}
	if l.output != nil {
		if _, err := c.cfg.Store.InvalidatedDataVersion(c.ctx, h.rec.Tenant, l.rec.Fingerprint); err == nil {
			l.rec.RunReason = "earlier data version invalidated"
			return nil
		}
	}
	l.rec.RunReason = c.rerunReason(h.rec.Tenant, fpi, l.memo.ttl)
	return nil
}

// checkReadableTimeout bounds checking a data version's files before reuse.
const checkReadableTimeout = 10 * time.Second

// checkReadable is errUnreadable unless p's manifest and files are still
// in the bucket: the catalog says committed, but files can be deleted
// outside moksha, and a reused data version that can't be read fails every step
// that reads it. It reads the bucket from the loop, like the store reads
// around it; a bucket that can't answer means rerun ("when unsure, rerun").
func (c *Core) checkReadable(p state.DataVersion) error {
	if c.cfg.DataVersions == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(c.ctx, checkReadableTimeout)
	defer cancel()
	err := c.cfg.DataVersions.Check(ctx, p.Location)
	if err == nil {
		return nil
	}
	c.log.Info("not reusing a data version that can't be read", "data_version", p.ID, "err", err)
	if errors.Is(err, dataversion.ErrMissingFiles) {
		return fmt.Errorf("%w: its files are gone", errUnreadable)
	}
	return fmt.Errorf("%w: %v", errUnreadable, err)
}

// lookupMemo finds what a run with l's fingerprint produced, completed at or
// after since.
func (c *Core) lookupMemo(tenant string, l *stepState, since time.Time) (memoMatch, error) {
	if l.output == nil {
		run, err := c.cfg.Store.MemoResult(c.ctx, tenant, l.rec.Fingerprint, since)
		if err != nil {
			return memoMatch{}, err
		}
		m := memoMatch{runID: run.RunID, seq: run.Seq, result: run.Result}
		// It may have built a data version in its implicit dataset (a dataframe):
		// then that data version is what gets reused, and it must be readable.
		switch p, err := c.cfg.Store.StepDataVersion(c.ctx, run.RunID, run.Seq); {
		case err == nil && p.State == state.DataVersionCommitted:
			if err := c.checkReadable(p); err != nil {
				return memoMatch{}, err
			}
			m.dataVersion = p.ID
		case err == nil:
			return memoMatch{}, fmt.Errorf("%w: data version %s is %s", errUnreadable, p.ID, p.State)
		case !errors.Is(err, state.ErrNotFound):
			return memoMatch{}, err
		}
		return m, nil
	}
	p, err := c.cfg.Store.MemoDataVersion(c.ctx, tenant, l.rec.Fingerprint, since)
	if err != nil {
		return memoMatch{}, err
	}
	if err := c.checkReadable(p); err != nil {
		return memoMatch{}, err
	}
	m := memoMatch{dataVersion: p.ID, runID: p.RunID, seq: p.StepSeq}
	if orig, err := c.cfg.Store.GetStep(c.ctx, p.RunID, p.StepSeq); err == nil {
		m.result = orig.Result
	}
	return m, nil
}

// recentRunsCompared is how many earlier runs of a step without an output
// rerunReason looks through for one with the same arguments.
const recentRunsCompared = 50

// rerunReason compares a step that has to run with an earlier run: whatever
// built the latest data version of its output partition or, for a step without an
// output, its latest run with the same arguments.
func (c *Core) rerunReason(tenant string, fpi fingerprintInputs, ttl time.Duration) string {
	old, ok := c.previousRun(tenant, fpi)
	if !ok {
		return "no previous run"
	}
	if old.Step != fpi.Step {
		return fmt.Sprintf("no previous run (the latest data version is from step %s)", old.Step)
	}
	var why []string
	if old.CodeVersion != fpi.CodeVersion {
		why = append(why, fmt.Sprintf("code changed (%s → %s)", old.CodeVersion, fpi.CodeVersion))
	}
	if !slices.Equal(old.Inputs, fpi.Inputs) {
		why = append(why, "input changed")
	}
	if !bytes.Equal(old.Args, fpi.Args) {
		why = append(why, "params changed")
	}
	if len(why) > 0 {
		return strings.Join(why, ", ")
	}
	if ttl > 0 {
		return fmt.Sprintf("expired (older than %s)", ttl)
	}
	return "no previous run"
}

// previousRun returns what went into the fingerprint of the run fpi is
// compared with; see rerunReason.
func (c *Core) previousRun(tenant string, fpi fingerprintInputs) (fingerprintInputs, bool) {
	var steps []state.Step
	if fpi.Dataset == "" {
		runs, err := c.cfg.Store.RecentStepRuns(c.ctx, tenant, fpi.Step, recentRunsCompared)
		if err != nil {
			return fingerprintInputs{}, false
		}
		steps = runs
	} else {
		prev, err := c.cfg.Store.LatestDataVersion(c.ctx, tenant, fpi.Dataset, fpi.Partition)
		if err != nil {
			return fingerprintInputs{}, false
		}
		step, err := c.cfg.Store.GetStep(c.ctx, prev.RunID, prev.StepSeq)
		if err != nil {
			return fingerprintInputs{}, false
		}
		steps = []state.Step{step}
	}
	for _, l := range steps {
		var old fingerprintInputs
		if len(l.FPInputs) == 0 || json.Unmarshal(l.FPInputs, &old) != nil {
			continue // built without a fingerprint
		}
		if fpi.Dataset != "" || bytes.Equal(old.Args, fpi.Args) {
			return old, true
		}
	}
	return fingerprintInputs{}, false
}
