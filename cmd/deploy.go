package cmd

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/api/gen/hubwagen/v1/hubwagenv1connect"
	"github.com/alper/hubwagen/internal/config"
	"github.com/alper/hubwagen/internal/deploycli"
)

var (
	deployServer     string
	deployFile       string
	deployDryRun     bool
	deployLocal      bool
	deployPython     string
	deployRuntime    string
	deployPush       bool
	deleteCancelRuns bool
)

var deployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "Hand this project to a server: it runs and scales its workers",
	Long: `Deploy hands a project to a server (docs/projects.md). Run it in the
project's directory: it reads the project file (hubwagen.yaml, or
hubwagen.project.yaml, with kind: Project), works out its build from the
code, and shows what deploying it changes against the code the server runs:
which steps run again because their code version changed, which are new,
which are no longer in the code, and which pipelines come or go.

Then it deploys: the server records the project's next version, registers
its build (its pipelines are accepted and its schedules apply at once),
starts the new version's pools (min workers, more when work waits) and
stops the old version's workers, which finish what they run.

Its workers are containers of its image, built from build.dockerfile with
docker or podman and tagged <build.image>:<build>. A server on this machine
uses the image as it is here; to a server elsewhere (or with --push) the
image is pushed to its registry with this machine's login and deployed by
digest, <build.image>@sha256:…, which the server pulls with its own
credentials (projects.auth_file). Without a Dockerfile, or with --local,
its code runs in place instead: python3 -m hubwagen.worker in this
directory, with PYTHONPATH as it is here, on a server on this machine.

env: {NAME: {secret: name}} in the project file sends only the secret's
name; the server holds the value (projects.secrets_dir) and refuses a
deploy that names a secret it doesn't have. The project's code runs here
and in its workers, never in the server.

--dry-run stops after showing what would change.`,
	Example: `  hubwagen deploy --dry-run
  hubwagen deploy --local --server http://127.0.0.1:7070`,
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE:         runDeploy,
}

func runDeploy(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	path, err := config.FindProject(deployFile, wd)
	if err != nil {
		return err
	}
	p, err := config.LoadProject(path)
	if err != nil {
		return err
	}
	opts := deploycli.Options{Local: deployLocal, Python: deployPython, Runtime: deployRuntime, Out: os.Stderr}
	local := localServer(deployServer)
	inPlace := p.Build.Dockerfile == "" || deployLocal
	if !deployDryRun && !local && inPlace {
		return fmt.Errorf("--server %s: an in-place deploy runs this directory, so the server must be on this machine; deploy an image (build.dockerfile, without --local) to a server elsewhere", deployServer)
	}
	v, src, err := deploycli.BuildVersions(ctx, p, opts)
	if err != nil {
		return err
	}
	server := strings.TrimSuffix(deployServer, "/")
	code, err := hubwagenv1connect.NewHaulServiceClient(http.DefaultClient, server).ListCode(ctx, connect.NewRequest(&hubwagenv1.ListCodeRequest{}))
	if err != nil {
		return serverError(err)
	}
	fmt.Print(deploycli.Report(p, src, deploycli.Compare(v, code.Msg)))
	if deployDryRun {
		fmt.Println("\nDry run: nothing was deployed.")
		return nil
	}

	reg, err := deploycli.Registration(ctx, p, src, opts)
	if err != nil {
		return err
	}
	if src.Image != "" {
		if err := deploycli.Tag(ctx, p, src, reg.BuildId, opts); err != nil {
			return err
		}
	}
	req, err := deploycli.DeployRequest(p, src, reg, deployPython, deployer())
	if err != nil {
		return err
	}
	if src.Image != "" && (deployPush || !local) {
		// A server elsewhere pulls it from the registry, by digest.
		fmt.Fprintf(os.Stderr, "pushing %s\n", deploycli.BuildImage(p, reg.BuildId))
		if req.Image, err = deploycli.Push(ctx, p, reg.BuildId, opts); err != nil {
			return err
		}
	}
	res, err := hubwagenv1connect.NewDeployServiceClient(http.DefaultClient, server).Deploy(ctx, connect.NewRequest(req))
	if err != nil {
		return serverError(err)
	}
	d := res.Msg.Deployment
	fmt.Printf("\nDeployed %s v%d (build %s).", d.Project, d.Version, d.BuildId)
	if d.Version > 1 {
		fmt.Printf(" The server starts its workers; v%d's finish what they run and stop.", d.Version-1)
	} else {
		fmt.Print(" The server starts its workers.")
	}
	fmt.Println(" hubwagen projects shows them.")
	return nil
}

// localServer reports whether a server URL is on this machine.
func localServer(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := u.Hostname()
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// deployer is who deploys, for the record: user@host.
func deployer() string {
	name := "unknown"
	if u, err := user.Current(); err == nil {
		name = u.Username
	}
	if h, err := os.Hostname(); err == nil {
		name += "@" + h
	}
	return name
}

var projectsCmd = &cobra.Command{
	Use:          "projects",
	Short:        "List the projects deployed to a server and their pools",
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		server := strings.TrimSuffix(deployServer, "/")
		withDeleted, _ := cmd.Flags().GetBool("deleted")
		ps, err := hubwagenv1connect.NewDeployServiceClient(http.DefaultClient, server).ListProjects(cmd.Context(),
			connect.NewRequest(&hubwagenv1.ListProjectsRequest{WithDeleted: withDeleted}))
		if err != nil {
			return serverError(err)
		}
		pools, err := hubwagenv1connect.NewHaulServiceClient(http.DefaultClient, server).ListPools(cmd.Context(),
			connect.NewRequest(&hubwagenv1.ListPoolsRequest{}))
		if err != nil {
			return serverError(err)
		}
		return deploycli.Projects(os.Stdout, ps.Msg.Projects, pools.Msg.Pools)
	},
}

var deleteCmd = &cobra.Command{
	Use:   "delete <project>",
	Short: "Delete a project from a server: its workers stop, its pipelines take no new runs",
	Long: `Delete stops a project on the server: its workers finish what they run and
stop, and its pipelines take no new runs. Its run history, data versions and
catalog stay; deploying it again brings it back as its next version.

With runs of its pipelines still open it refuses; --cancel-runs cancels them.`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		res, err := hubwagenv1connect.NewDeployServiceClient(http.DefaultClient, strings.TrimSuffix(deployServer, "/")).DeleteProject(
			cmd.Context(), connect.NewRequest(&hubwagenv1.DeleteProjectRequest{Project: args[0], By: deployer(), CancelRuns: deleteCancelRuns}))
		if err != nil {
			if connect.CodeOf(err) == connect.CodeFailedPrecondition && !deleteCancelRuns {
				return errors.New(strings.TrimPrefix(err.Error(), "failed_precondition: ") + " (--cancel-runs)")
			}
			return serverError(err)
		}
		fmt.Printf("Deleted %s: its workers finish what they run and stop.", args[0])
		if n := res.Msg.CancelledRuns; n > 0 {
			fmt.Printf(" %d open run(s) cancelled.", n)
		}
		fmt.Println()
		return nil
	},
}

func init() {
	for _, c := range []*cobra.Command{deployCmd, projectsCmd, deleteCmd} {
		c.Flags().StringVar(&deployServer, "server", "http://127.0.0.1:7070", "hubwagen API")
		rootCmd.AddCommand(c)
	}
	deployCmd.Flags().StringVar(&deployFile, "file", "", "project file (default: hubwagen.yaml or hubwagen.project.yaml with kind: Project)")
	deployCmd.Flags().BoolVar(&deployDryRun, "dry-run", false, "only show what would change")
	deployCmd.Flags().BoolVar(&deployLocal, "local", false, "run the code in place, not in the project's image")
	deployCmd.Flags().StringVar(&deployPython, "python", "python3", "the interpreter in place (--local)")
	deployCmd.Flags().StringVar(&deployRuntime, "runtime", "", "docker or podman (default: the one on PATH)")
	deployCmd.Flags().BoolVar(&deployPush, "push", false, "push the image to its registry and deploy it by digest (always, to a server elsewhere)")
	projectsCmd.Flags().Bool("deleted", false, "also list deleted projects")
	deleteCmd.Flags().BoolVar(&deleteCancelRuns, "cancel-runs", false, "cancel the project's open runs")
}
