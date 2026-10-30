package cmd

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/api/gen/hubwagen/v1/hubwagenv1connect"
	"github.com/alper/hubwagen/internal/config"
	"github.com/alper/hubwagen/internal/deploycli"
)

var (
	deployServer  string
	deployFile    string
	deployDryRun  bool
	deployLocal   bool
	deployPython  string
	deployRuntime string
)

var deployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "Show what deploying this project would change on a server (--dry-run)",
	Long: `Deploy hands a project to a server (docs/projects.md). Run it in the
project's directory: it reads the project file (hubwagen.yaml, or
hubwagen.project.yaml, with kind: Project), works out its build from the
code, and compares it with the code the server runs: which steps run again
because their code changed, which are new, which are no longer in the
code, and which pipelines come or go.

The build is worked out in the project's image (built from build.dockerfile
with docker or podman, tagged <build.image>:dry-run), or, without a
Dockerfile or with --local, in place with python3 -m hubwagen.worker
--versions and the current environment. The project's code runs here, never
on the server.

For now only --dry-run: nothing is deployed. Deploying comes next.`,
	Example: `  hubwagen deploy --dry-run
  hubwagen deploy --dry-run --local --server http://127.0.0.1:7070`,
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		if !deployDryRun {
			return errors.New("only hubwagen deploy --dry-run for now: deploying to a server comes with step 2 of docs/projects.md")
		}
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
		v, src, err := deploycli.BuildVersions(cmd.Context(), p, deploycli.Options{Local: deployLocal, Python: deployPython,
			Runtime: deployRuntime, Out: os.Stderr})
		if err != nil {
			return err
		}
		c := hubwagenv1connect.NewHaulServiceClient(http.DefaultClient, strings.TrimSuffix(deployServer, "/"))
		code, err := c.ListCode(cmd.Context(), connect.NewRequest(&hubwagenv1.ListCodeRequest{}))
		if err != nil {
			return serverError(err)
		}
		fmt.Print(deploycli.Report(p, src, deploycli.Compare(v, code.Msg)))
		fmt.Println("\nDry run: nothing was deployed.")
		return nil
	},
}

func init() {
	deployCmd.Flags().StringVar(&deployServer, "server", "http://127.0.0.1:7070", "hubwagen API to compare with")
	deployCmd.Flags().StringVar(&deployFile, "file", "", "project file (default: hubwagen.yaml or hubwagen.project.yaml with kind: Project)")
	deployCmd.Flags().BoolVar(&deployDryRun, "dry-run", false, "only show what would change (required for now)")
	deployCmd.Flags().BoolVar(&deployLocal, "local", false, "work out the build in place, not in the project's image")
	deployCmd.Flags().StringVar(&deployPython, "python", "python3", "the interpreter for --local")
	deployCmd.Flags().StringVar(&deployRuntime, "runtime", "", "docker or podman (default: the one on PATH)")
	rootCmd.AddCommand(deployCmd)
}
