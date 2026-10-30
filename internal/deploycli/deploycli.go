// Package deploycli is what `hubwagen deploy` does (docs/projects.md). For
// now, the dry run: work out a project's build from its code (in its image,
// or in place), compare it with the code the server runs (ListCode), and
// tell what a deploy would change, in plain words.
package deploycli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strings"
	"text/tabwriter"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/config"
)

// Versions is a build as the worker reports it (python -m hubwagen.worker
// --versions): its ID, and the code version of each pipeline and step.
type Versions struct {
	Build string            `json:"build"`
	Hauls map[string]codeOf `json:"hauls"`
	Lifts map[string]codeOf `json:"lifts"`
}

type codeOf struct {
	Version string `json:"version"`
}

// Source says where the build was worked out, for the report.
type Source struct {
	Image string // the image it ran in; "" in place
}

// Options are how to get the versions.
type Options struct {
	Local   bool   // in place even with a Dockerfile
	Python  string // the interpreter in place; default python3
	Runtime string // docker or podman; default the one on PATH
	Out     io.Writer
}

// BuildVersions works out p's build: in its image (built from its
// Dockerfile, tagged <image>:dry-run) unless it has none or opts.Local;
// else in place, with the project's environment. The project's code runs
// here, never on the server.
func BuildVersions(ctx context.Context, p *config.Project, opts Options) (Versions, Source, error) {
	if p.Build.Dockerfile == "" || opts.Local {
		cmd := exec.CommandContext(ctx, or(opts.Python, "python3"), "-m", "hubwagen.worker", "--versions")
		cmd.Dir = p.Dir
		v, err := run(cmd, opts.Out)
		return v, Source{}, err
	}
	rt := runtime(opts)
	image := p.Build.Image + ":dry-run"
	args := []string{"build", "-t", image, "-f", p.Build.Dockerfile}
	for _, k := range slices.Sorted(maps.Keys(p.Build.Args)) {
		args = append(args, "--build-arg", k+"="+p.Build.Args[k])
	}
	build := exec.CommandContext(ctx, rt, append(args, p.Build.Context)...)
	build.Stdout, build.Stderr = opts.Out, opts.Out
	if err := build.Run(); err != nil {
		return Versions{}, Source{}, fmt.Errorf("%s build: %w", rt, err)
	}
	v, err := run(exec.CommandContext(ctx, rt, "run", "--rm", image, "python3", "-m", "hubwagen.worker", "--versions"), opts.Out)
	return v, Source{Image: image}, err
}

func run(cmd *exec.Cmd, stderr io.Writer) (Versions, error) {
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, stderr
	if cmd.Stderr == nil {
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Run(); err != nil {
		return Versions{}, fmt.Errorf("%s: %w", strings.Join(cmd.Args, " "), err)
	}
	var v Versions
	if err := json.Unmarshal(out.Bytes(), &v); err != nil || v.Build == "" {
		return Versions{}, fmt.Errorf("%s: not a build's versions: %s", strings.Join(cmd.Args, " "), firstLine(out.String()))
	}
	return v, nil
}

func firstLine(s string) string {
	s, _, _ = strings.Cut(strings.TrimSpace(s), "\n")
	return s
}

// Change is what a deploy does to one step or pipeline.
type Change string

const (
	Unchanged Change = "unchanged"
	Changed   Change = "changed" // steps: a new code version
	Added     Change = "new"
	Removed   Change = "removed" // the server has it; this code doesn't
)

// Item is one step or pipeline in a diff.
type Item struct {
	Name     string
	Change   Change
	From, To string // code versions (steps)
	BuildID  string // removed: the server's build that has it
}

// Diff is what deploying a build would change on a server.
type Diff struct {
	Build     string
	Newest    *hubwagenv1.Code // the server's newest build, if any
	Steps     []Item
	Pipelines []Item
}

// Compare compares v with the code the server runs.
func Compare(v Versions, server *hubwagenv1.ListCodeResponse) Diff {
	d := Diff{Build: v.Build}
	for _, c := range append(slices.Clone(server.GetSteps()), server.GetPipelines()...) {
		if d.Newest == nil || c.RegisteredAt.AsTime().After(d.Newest.RegisteredAt.AsTime()) {
			d.Newest = c
		}
	}
	d.Steps = compare(v.Lifts, server.GetSteps(), true)
	d.Pipelines = compare(v.Hauls, server.GetPipelines(), false)
	return d
}

func compare(local map[string]codeOf, server []*hubwagenv1.Code, versions bool) []Item {
	have := map[string]*hubwagenv1.Code{}
	for _, c := range server {
		have[c.Name] = c
	}
	var out []Item
	for _, name := range slices.Sorted(maps.Keys(local)) {
		c, ok := have[name]
		switch {
		case !ok:
			out = append(out, Item{Name: name, Change: Added, To: local[name].Version})
		case versions && c.CodeVersion != local[name].Version:
			out = append(out, Item{Name: name, Change: Changed, From: c.CodeVersion, To: local[name].Version})
		default:
			out = append(out, Item{Name: name, Change: Unchanged, To: local[name].Version})
		}
	}
	for _, c := range server {
		if _, ok := local[c.Name]; !ok {
			out = append(out, Item{Name: c.Name, Change: Removed, From: c.CodeVersion, BuildID: c.BuildId})
		}
	}
	return out
}

// Counts is how many steps change how.
func (d Diff) Counts() map[Change]int {
	n := map[Change]int{}
	for _, it := range d.Steps {
		n[it.Change]++
	}
	return n
}

// Report tells the diff, the project's pools and where the build came
// from, in plain words; times in UTC.
func Report(p *config.Project, src Source, d Diff) string {
	var b strings.Builder
	where := "its code in place"
	if src.Image != "" {
		where = "image " + src.Image
	}
	fmt.Fprintf(&b, "Project %s: build %s (from %s)\n", p.Name, d.Build, where)
	if d.Newest == nil {
		fmt.Fprintln(&b, "The server has no builds yet: every step and pipeline is new.")
	} else {
		fmt.Fprintf(&b, "The server's newest build: %s, registered %s\n", d.Newest.BuildId,
			d.Newest.RegisteredAt.AsTime().UTC().Format("2006-01-02 15:04 UTC"))
	}

	n := d.Counts()
	var parts []string
	for _, c := range []struct {
		ch        Change
		one, many string
	}{{Changed, "runs again", "run again"}, {Added, "new", "new"}, {Unchanged, "unchanged", "unchanged"},
		{Removed, "no longer in the code", "no longer in the code"}} {
		switch n[c.ch] {
		case 0:
		case 1:
			parts = append(parts, "1 "+c.one)
		default:
			parts = append(parts, fmt.Sprintf("%d %s", n[c.ch], c.many))
		}
	}
	fmt.Fprintf(&b, "\nSteps: %s\n", strings.Join(parts, ", "))
	tw := tabwriter.NewWriter(&b, 0, 0, 3, ' ', 0)
	for _, it := range d.Steps {
		fmt.Fprintf(tw, "  %s\t%s\n", it.Name, stepText(it))
	}
	tw.Flush()
	if n[Changed] > 0 {
		fmt.Fprintln(&b, "  (A code version covers the step's code, the packages it uses and the Python version.")
		fmt.Fprintln(&b, "  Steps that read what a changed step writes run again too, when it writes something new.)")
	}
	if changed := slices.ContainsFunc(d.Pipelines, func(it Item) bool { return it.Change != Unchanged }); changed {
		fmt.Fprintln(&b, "\nPipelines")
		tw = tabwriter.NewWriter(&b, 0, 0, 3, ' ', 0)
		for _, it := range d.Pipelines {
			if it.Change == Unchanged {
				continue
			}
			text := "new"
			if it.Change == Removed {
				text = "not in this code: no new runs of it once deployed"
			}
			fmt.Fprintf(tw, "  %s\t%s\n", it.Name, text)
		}
		tw.Flush()
	}

	fmt.Fprintln(&b, "\nPools")
	tw = tabwriter.NewWriter(&b, 0, 0, 3, ' ', 0)
	for _, pool := range p.Pools {
		lo, hi := pool.Bounds()
		count := fmt.Sprintf("%d–%d workers", lo, hi)
		if lo == hi {
			count = fmt.Sprintf("%d workers", lo)
		}
		each := fmt.Sprintf("%d steps", pool.LiftCount())
		if pool.LiftCount() == 1 {
			each = "1 step"
		}
		fmt.Fprintf(tw, "  %s\t%s, %s at once each, docks %s\n", pool.Name, count, each, strings.Join(pool.DockList(), ", "))
	}
	tw.Flush()
	return b.String()
}

func stepText(it Item) string {
	switch it.Change {
	case Changed:
		return fmt.Sprintf("code version changed (%s → %s): runs again, also for inputs it ran with before", it.From, it.To)
	case Added:
		return "new: runs"
	case Removed:
		return fmt.Sprintf("not in this code (the server has it from %s)", it.BuildID)
	}
	return "unchanged: reused for inputs it ran with before"
}

// Registration is p's build registration, what its workers register
// (python -m hubwagen.worker --build-json), worked out where BuildVersions
// worked out its versions: in src.Image, or in place.
func Registration(ctx context.Context, p *config.Project, src Source, opts Options) (*hubwagenv1.RegisterBuildRequest, error) {
	var cmd *exec.Cmd
	if src.Image == "" {
		cmd = exec.CommandContext(ctx, or(opts.Python, "python3"), "-m", "hubwagen.worker", "--build-json")
		cmd.Dir = p.Dir
	} else {
		cmd = exec.CommandContext(ctx, runtime(opts), "run", "--rm", src.Image, "python3", "-m", "hubwagen.worker", "--build-json")
	}
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, io.MultiWriter(opts.Out, &errOut)
	if err := cmd.Run(); err != nil {
		if strings.Contains(errOut.String(), "unrecognized arguments: --build-json") {
			return nil, fmt.Errorf("%s: its hubwagen SDK is older than this hubwagen (no --build-json): build it on a newer hubwagen-worker image, or deploy --local",
				strings.Join(cmd.Args, " "))
		}
		return nil, fmt.Errorf("%s: %w", strings.Join(cmd.Args, " "), err)
	}
	var reg hubwagenv1.RegisterBuildRequest
	if err := protojson.Unmarshal(out.Bytes(), &reg); err != nil || reg.BuildId == "" {
		return nil, fmt.Errorf("%s: not a build registration: %s", strings.Join(cmd.Args, " "), firstLine(out.String()))
	}
	return &reg, nil
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func runtime(opts Options) string {
	if opts.Runtime != "" {
		return opts.Runtime
	}
	if _, err := exec.LookPath("docker"); err == nil {
		return "docker"
	}
	return "podman"
}

// DeployRequest is p's deployment of the build reg: its image tagged with
// the build (Tag it first), or its directory and interpreter in place, with
// PYTHONPATH as it is here. Secrets in env aren't supported yet.
func DeployRequest(p *config.Project, src Source, reg *hubwagenv1.RegisterBuildRequest, python, by string) (*hubwagenv1.DeployRequest, error) {
	req := &hubwagenv1.DeployRequest{Project: p.Name, Build: reg, By: by, Env: map[string]string{}}
	for _, k := range slices.Sorted(maps.Keys(p.Env)) {
		e := p.Env[k]
		if e.Secret != "" {
			return nil, fmt.Errorf("env.%s: {secret: %s}: secrets come with step 3 of docs/projects.md; use a plain value for now", k, e.Secret)
		}
		req.Env[k] = e.Value
	}
	if src.Image != "" {
		req.Image = BuildImage(p, reg.BuildId)
	} else {
		py, err := exec.LookPath(python)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", python, err)
		}
		req.Dir, req.Python = p.Dir, py
		if pp := os.Getenv("PYTHONPATH"); pp != "" {
			if _, set := req.Env["PYTHONPATH"]; !set {
				req.Env["PYTHONPATH"] = pp
			}
		}
	}
	for _, pool := range p.Pools {
		lo, hi := pool.Bounds()
		req.Pools = append(req.Pools, &hubwagenv1.DeployedPool{Name: pool.Name, Docks: pool.DockList(), Min: int32(lo), Max: int32(hi),
			Lifts: int32(pool.LiftCount()), IdleAfter: durationpb.New(pool.Idle())})
	}
	return req, nil
}

// BuildImage is the image a build of p is deployed as: <image>:<build>.
func BuildImage(p *config.Project, build string) string { return p.Build.Image + ":" + build }

// Tag tags the image the versions were worked out in (src.Image) as the
// build's, BuildImage.
func Tag(ctx context.Context, p *config.Project, src Source, build string, opts Options) error {
	cmd := exec.CommandContext(ctx, runtime(opts), "tag", src.Image, BuildImage(p, build))
	cmd.Stdout, cmd.Stderr = opts.Out, opts.Out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w", strings.Join(cmd.Args, " "), err)
	}
	return nil
}

// Projects writes the server's projects as a table: version, build, where
// their workers run, and each pool with the workers it runs now
// (ListPools, pools named <project>-<pool>-v<version>). Times in UTC.
func Projects(w io.Writer, ps []*hubwagenv1.Deployment, pools []*hubwagenv1.Pool) error {
	if len(ps) == 0 {
		_, err := fmt.Fprintln(w, "no projects (hubwagen deploy in a project's directory hands one to the server)")
		return err
	}
	running := map[string]int{}
	for _, p := range pools {
		running[p.Name] = len(p.Workers)
	}
	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	fmt.Fprintln(tw, "PROJECT\tVERSION\tBUILD\tRUNS\tPOOLS\tDEPLOYED\tBY")
	for _, p := range ps {
		where := "image " + p.Image
		if p.Image == "" {
			where = "in place, " + shortDir(p.Dir)
		}
		var pp []string
		for _, pool := range p.Pools {
			n := running[fmt.Sprintf("%s-%s-v%d", p.Project, pool.Name, p.Version)]
			pp = append(pp, fmt.Sprintf("%s %d of %d–%d", pool.Name, n, pool.Min, pool.Max))
		}
		version := fmt.Sprintf("v%d", p.Version)
		if p.DeletedAt != nil {
			version += " (deleted)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", p.Project, version, p.BuildId, where, strings.Join(pp, ", "),
			p.DeployedAt.AsTime().UTC().Format("2006-01-02 15:04 UTC"), p.By)
	}
	return tw.Flush()
}

// shortDir is a directory's last two elements, for a table: …/examples/revenue.
func shortDir(dir string) string {
	parts := strings.Split(strings.TrimSuffix(dir, "/"), "/")
	if len(parts) <= 3 {
		return dir
	}
	return "…/" + strings.Join(parts[len(parts)-2:], "/")
}
