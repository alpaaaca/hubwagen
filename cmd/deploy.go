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
docker or podman and tagged <build.image>:<build>; or, without a
Dockerfile or with --local, its code in place: python3 -m hubwagen.worker
in this directory, with PYTHONPATH as it is here. Either way the server
must be on this machine for now (pushing to a registry comes next). The
project's code runs here and in its workers, never in the server.

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
	if !deployDryRun && !localServer(deployServer) {
		return fmt.Errorf("--server %s: deploying needs a server on this machine for now: it runs this machine's image or directory (pushing to a registry comes with step 3 of docs/projects.md); --dry-run works with any", deployServer)
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
	projectsCmd.Flags().Bool("deleted", false, "also list deleted projects")
	deleteCmd.Flags().BoolVar(&deleteCancelRuns, "cancel-runs", false, "cancel the project's open runs")
}
