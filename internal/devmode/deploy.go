package devmode

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/api/gen/moksha/v1/mokshav1connect"
	"github.com/alper/moksha/internal/config"
	"github.com/alper/moksha/internal/deploycli"
)

// Deployer is moksha dev's deploy (docs/projects.md, step 4): it deploys
// a project's directory in place to the dev server, its workers processes
// there, and deploys again when its code changes. The project's code runs
// here (python -m moksha.worker --build-json), never in the server.
type Deployer struct {
	Path   string // the project file
	Server string // the dev server's API
	Python string // the interpreter in place
	By     string // who deploys, for the record
	Print  func(string)
	Log    io.Writer // what the worker prints while it works out the build

	last *mokshav1.DeployRequest // what this dev deployed last
}

// Deploy deploys the project unless the server already runs it as it is:
// the same build, directory, pools and env. It prints what changed.
func (d *Deployer) Deploy(ctx context.Context) error {
	p, err := config.LoadProject(d.Path)
	if err != nil {
		return err
	}
	opts := deploycli.Options{Local: true, Python: d.Python, Out: d.Log}
	reg, err := deploycli.Registration(ctx, p, deploycli.Source{}, opts)
	if err != nil {
		return err
	}
	req, err := deploycli.DeployRequest(p, deploycli.Source{}, reg, d.Python, d.By)
	if err != nil {
		return err
	}
	if d.last != nil && proto.Equal(d.last, req) {
		return nil
	}
	deploys := mokshav1connect.NewDeployServiceClient(http.DefaultClient, d.Server)
	if d.last == nil {
		// A dev started again: the server may run this already. Plain env
		// isn't in a deployment, so a change to it alone while dev was
		// stopped waits for the next change to the code.
		ps, err := deploys.ListProjects(ctx, connect.NewRequest(&mokshav1.ListProjectsRequest{}))
		if err != nil {
			return err
		}
		for _, cur := range ps.Msg.Projects {
			if cur.Project == p.Name && runsAsIs(cur, req) {
				d.last = req
				d.Print(fmt.Sprintf("project %s v%d: deployed already (build %s); dev deploys again when its code changes", p.Name, cur.Version, cur.BuildId))
				return nil
			}
		}
	}
	code, err := mokshav1connect.NewRunServiceClient(http.DefaultClient, d.Server).ListCode(ctx, connect.NewRequest(&mokshav1.ListCodeRequest{}))
	if err != nil {
		return err
	}
	diff := deploycli.Compare(deploycli.VersionsOf(reg), code.Msg)
	res, err := deploys.Deploy(ctx, connect.NewRequest(req))
	if err != nil {
		return err
	}
	d.last = req
	dep := res.Msg.Deployment
	d.Print(fmt.Sprintf("project %s v%d deployed (build %s, in place): %s", dep.Project, dep.Version, dep.BuildId, changes(diff)))
	return nil
}

// runsAsIs reports whether the server's deployment cur runs req: in
// place, with the same build, directory, pools and secrets.
func runsAsIs(cur *mokshav1.Deployment, req *mokshav1.DeployRequest) bool {
	return cur.DeletedAt == nil && cur.Image == "" && cur.BuildId == req.Build.BuildId && cur.Dir == req.Dir &&
		slices.EqualFunc(cur.Pools, req.Pools, func(a, b *mokshav1.DeployedPool) bool { return proto.Equal(a, b) }) &&
		maps.Equal(cur.Secrets, req.Secrets)
}

// changes is what a deploy changed, in a few words: the steps that run
// again or are new, the pipelines that come or go.
func changes(d deploycli.Diff) string {
	if d.Newest == nil {
		return fmt.Sprintf("%s, %s; its workers start", count(len(d.Steps), "step"), count(len(d.Pipelines), "pipeline"))
	}
	var parts []string
	for _, it := range d.Steps {
		switch it.Change {
		case deploycli.Changed:
			parts = append(parts, it.Name+" runs again (its code changed)")
		case deploycli.Added:
			parts = append(parts, it.Name+" is new")
		}
	}
	for _, it := range d.Pipelines {
		switch it.Change {
		case deploycli.Added:
			parts = append(parts, "pipeline "+it.Name+" is new")
		case deploycli.Removed:
			parts = append(parts, "pipeline "+it.Name+" takes no new runs")
		}
	}
	if len(parts) == 0 {
		return "no step's code changed; its workers restart"
	}
	return strings.Join(parts, ", ")
}

func count(n int, what string) string {
	if n == 1 {
		return "1 " + what
	}
	return fmt.Sprintf("%d %ss", n, what)
}

// Watch deploys again when the project's code or project file changes.
// Every interval it looks at the Python files, pyproject.toml and the
// project file under the project's directory (not in hidden directories,
// __pycache__, node_modules or virtual environments), and deploys once
// they have stayed the same for one look, so a save of several files is
// one deploy. A deploy that fails is printed: the next change tries again.
func (d *Deployer) Watch(ctx context.Context, every time.Duration) {
	dir, name := filepath.Dir(d.Path), filepath.Base(d.Path)
	seen := codeState(dir, name)
	pending := false
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := codeState(dir, name)
		switch {
		case now != seen:
			seen, pending = now, true
		case pending:
			pending = false
			if err := d.Deploy(ctx); err != nil && ctx.Err() == nil {
				d.Print(fmt.Sprintf("can't deploy %s: %v (dev tries again when it changes)", name, err))
			}
		}
	}
}

// codeState is a hash of the names, sizes and modification times of the
// files Watch looks at.
func codeState(dir, projectFile string) uint64 {
	h := fnv.New64a()
	_ = filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		name := e.Name()
		if e.IsDir() {
			if path != dir && (strings.HasPrefix(name, ".") || name == "__pycache__" || name == "node_modules" || isVenv(path)) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".py") && name != "pyproject.toml" && (path != filepath.Join(dir, projectFile)) {
			return nil
		}
		info, err := e.Info()
		if err != nil {
			return nil
		}
		fmt.Fprintf(h, "%s\x00%d\x00%d\x00", path, info.Size(), info.ModTime().UnixNano())
		return nil
	})
	return h.Sum64()
}

func isVenv(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "pyvenv.cfg"))
	return err == nil
}
