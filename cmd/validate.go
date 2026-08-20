package cmd

import (
	"fmt"
	"os"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/spf13/cobra"
)

var validateCmd = &cobra.Command{
	Use:          "validate <dag-file>",
	Short:        "Validate a DAG YAML file without executing it",
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE:         validateDAG,
}

func init() {
	rootCmd.AddCommand(validateCmd)
}

func validateDAG(_ *cobra.Command, args []string) error {
	path := args[0]

	d, err := dag.ParseFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse error: %v\n", err)
		return err
	}

	errs := dag.Validate(d)
	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(os.Stderr, "error: %v\n", e)
		}
		return fmt.Errorf("%d validation error(s)", len(errs))
	}

	fmt.Printf("✓ %s is valid (%d steps)\n", path, len(d.Steps))
	return nil
}
