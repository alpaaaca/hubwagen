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

	"github.com/alper/hubwagen/internal/state"
)

// Memoization. A lift that opts in (Command.Memo) gets a fingerprint: a hash
// of its name, its code version in the build that replayed the haul, its
// input pallets' content, its arguments and its output, if any. If what an
// earlier run with that fingerprint produced is still there (and young
// enough, for a TTL), the lift is not run: the haul continues with it. For a
// lift with an output that is a committed pallet; for one without, the
// result of a lift that ran and completed, which the store always has.
// Otherwise the lift runs, and its scheduling records why (RunReason).
//
// Arguments are part of the fingerprint as canonical JSON (keys sorted), so
// the SDK passes pallets as Inputs, not inside Args: then an identical
// rebuild upstream (same content hash) doesn't change the fingerprint.

// BuildRequest registers a worker build: the hauls it defines and the code
// version of each lift.
type BuildRequest struct {
	ID            string
	Hauls         []string
	Lifts         map[string]string  // lift name → code version
	Schedules     []ScheduleSpec     // for hauls it defines; see schedules.go
	Subscriptions []SubscriptionSpec // for hauls it defines; see subscriptions.go
}

// RegisterBuild records a build. Registering the same build again is a
// no-op; reusing a build ID with other hauls or versions is an error, since
// builds are immutable (a build recorded before hauls were, gets its hauls
// added). A new build gives stuck hauls a replay: it may carry the fix.
//
// Workers that poll with a build ID get only the haul tasks and lifts their
// build defines, so projects with different code can share docks.
func (c *Core) RegisterBuild(ctx context.Context, req BuildRequest) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) {
		if req.ID == "" {
			return struct{}{}, fmt.Errorf("%w: a build needs an ID", ErrInvalid)
		}
		hauls := slices.Compact(slices.Sorted(slices.Values(req.Hauls)))
		if slices.Contains(hauls, "") {
			return struct{}{}, fmt.Errorf("%w: build %s: haul names must not be empty", ErrInvalid, req.ID)
		}
		if old, ok := c.builds[req.ID]; ok {
			if !mapsEqual(old, req.Lifts) {
				return struct{}{}, fmt.Errorf("%w: build %s is already registered with other code versions", ErrInvalid, req.ID)
			}
			known := c.buildHauls[req.ID]
			switch {
			case len(known) == 0 && len(hauls) > 0: // recorded before hauls were
			case !setEqual(known, hauls):
				return struct{}{}, fmt.Errorf("%w: build %s is already registered with other hauls", ErrInvalid, req.ID)
			default:
				return struct{}{}, nil
			}
		}
		schedules, err := validateSchedules(hauls, req.Schedules)
		if err != nil {
			return struct{}{}, fmt.Errorf("build %s: %w", req.ID, err)
		}
		subs, err := validateSubscriptions(hauls, req.Subscriptions)
		if err != nil {
			return struct{}{}, fmt.Errorf("build %s: %w", req.ID, err)
		}
		lifts := map[string]string{}
		for k, v := range req.Lifts {
			if k == "" || v == "" {
				return struct{}{}, fmt.Errorf("%w: build %s: lift names and code versions must not be empty", ErrInvalid, req.ID)
			}
			lifts[k] = v
		}
		c.addBuild(req.ID, lifts, hauls)
		b := state.Build{ID: req.ID, Hauls: hauls, Lifts: lifts, RegisteredAt: c.now()}
		c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutBuild(b) })
		c.emit(state.EventBuildRegistered, "", buildRegisteredPayload{ID: req.ID, Hauls: hauls, Lifts: lifts})
		c.applySchedules(req.ID, hauls, schedules)
		c.applySubscriptions(req.ID, hauls, subs)
		c.resumeStuckHauls()
		return struct{}{}, nil
	})
	return err
}

func (c *Core) addBuild(id string, lifts map[string]string, hauls []string) {
	c.builds[id] = lifts
	set := map[string]bool{}
	for _, h := range hauls {
		set[h] = true
		c.haulNames[h] = true
	}
	c.buildHauls[id] = set
}

// canRun reports whether a worker of build can take t: its build defines the
// haul or lift. Pollers without a build ID, and builds recorded without
// hauls, take any haul task.
func (c *Core) canRun(build string, t Task) bool {
	if build == "" {
		return true
	}
	if t.Kind == TaskHaul {
		hauls := c.buildHauls[build]
		return len(hauls) == 0 || hauls[t.Name]
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
// lift, so a later run can say what changed.
type fingerprintInputs struct {
	Lift        string          `json:"lift"`
	CodeVersion string          `json:"code_version"`
	Inputs      []inputKey      `json:"inputs,omitempty"`
	Args        json.RawMessage `json:"args,omitempty"` // canonical
	Dataset     string          `json:"dataset"`
	Partition   string          `json:"partition,omitempty"`
}

type inputKey struct {
	Name string `json:"name,omitempty"`
	Key  string `json:"key"` // content hash, else pallet ID
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
var errUnreadable = errors.New("not readable")

// memoMatch is what a reused lift completes with: the pallet (if it has an
// output) and result of the lift that ran with the same fingerprint.
type memoMatch struct {
	pallet string
	haulID string
	seq    int
	result json.RawMessage
}

// memoize computes l's fingerprint and looks for something to reuse. It sets
// the lift's fingerprint and, when the lift has to run, its RunReason.
func (c *Core) memoize(l *liftState, inputs []state.Pallet) *memoMatch {
	h := l.haul
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
	fpi := fingerprintInputs{Lift: l.rec.Name, CodeVersion: version, Args: args}
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
	l.rec.FPInputs = marshalPayload(state.EventLiftScheduled, fpi)

	var since time.Time
	if l.memo.ttl > 0 {
		since = c.now().Add(-l.memo.ttl)
	}
	m, err := c.lookupMemo(h.rec.Tenant, l, since)
	switch {
	case err == nil:
		return &m
	case errors.Is(err, errUnreadable):
		l.rec.RunReason = fmt.Sprintf("the earlier run's output can't be read (%v)", err)
		return nil
	case !errors.Is(err, state.ErrNotFound):
		c.log.Warn("memo lookup failed", "haul", h.rec.ID, "seq", l.rec.Seq, "err", err)
		l.rec.RunReason = "memo lookup failed"
		return nil
	}
	l.rec.RunReason = c.rerunReason(h.rec.Tenant, fpi, l.memo.ttl)
	return nil
}

// lookupMemo finds what a run with l's fingerprint produced, completed at or
// after since.
func (c *Core) lookupMemo(tenant string, l *liftState, since time.Time) (memoMatch, error) {
	if l.output == nil {
		run, err := c.cfg.Store.MemoResult(c.ctx, tenant, l.rec.Fingerprint, since)
		if err != nil {
			return memoMatch{}, err
		}
		m := memoMatch{haulID: run.HaulID, seq: run.Seq, result: run.Result}
		// It may have built a pallet in its implicit dataset (a dataframe):
		// then that pallet is what gets reused, and it must be readable.
		switch p, err := c.cfg.Store.LiftPallet(c.ctx, run.HaulID, run.Seq); {
		case err == nil && p.State == state.PalletCommitted:
			m.pallet = p.ID
		case err == nil:
			return memoMatch{}, fmt.Errorf("%w: pallet %s is %s", errUnreadable, p.ID, p.State)
		case !errors.Is(err, state.ErrNotFound):
			return memoMatch{}, err
		}
		return m, nil
	}
	p, err := c.cfg.Store.MemoPallet(c.ctx, tenant, l.rec.Fingerprint, since)
	if err != nil {
		return memoMatch{}, err
	}
	m := memoMatch{pallet: p.ID, haulID: p.HaulID, seq: p.LiftSeq}
	if orig, err := c.cfg.Store.GetLift(c.ctx, p.HaulID, p.LiftSeq); err == nil {
		m.result = orig.Result
	}
	return m, nil
}

// recentRunsCompared is how many earlier runs of a lift without an output
// rerunReason looks through for one with the same arguments.
const recentRunsCompared = 50

// rerunReason compares a lift that has to run with an earlier run: whatever
// built the latest pallet of its output partition or, for a lift without an
// output, its latest run with the same arguments.
func (c *Core) rerunReason(tenant string, fpi fingerprintInputs, ttl time.Duration) string {
	old, ok := c.previousRun(tenant, fpi)
	if !ok {
		return "no previous run"
	}
	if old.Lift != fpi.Lift {
		return fmt.Sprintf("no previous run (the latest pallet is from lift %s)", old.Lift)
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
	var lifts []state.Lift
	if fpi.Dataset == "" {
		runs, err := c.cfg.Store.RecentLiftRuns(c.ctx, tenant, fpi.Lift, recentRunsCompared)
		if err != nil {
			return fingerprintInputs{}, false
		}
		lifts = runs
	} else {
		prev, err := c.cfg.Store.LatestPallet(c.ctx, tenant, fpi.Dataset, fpi.Partition)
		if err != nil {
			return fingerprintInputs{}, false
		}
		lift, err := c.cfg.Store.GetLift(c.ctx, prev.HaulID, prev.LiftSeq)
		if err != nil {
			return fingerprintInputs{}, false
		}
		lifts = []state.Lift{lift}
	}
	for _, l := range lifts {
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
