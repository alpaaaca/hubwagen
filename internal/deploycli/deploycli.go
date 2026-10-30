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
		py := opts.Python
		if py == "" {
			py = "python3"
		}
		cmd := exec.CommandContext(ctx, py, "-m", "hubwagen.worker", "--versions")
		cmd.Dir = p.Dir
		v, err := run(cmd, opts.Out)
		return v, Source{}, err
	}
	rt := opts.Runtime
	if rt == "" {
		rt = "podman"
		if _, err := exec.LookPath("docker"); err == nil {
			rt = "docker"
		}
	}
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
