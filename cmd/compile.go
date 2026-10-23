package cmd

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/alper/hubwagen/internal/spec"
	"github.com/spf13/cobra"
)

var (
	compileOut       string
	compileCheck     bool
	compileNoResolve bool
	compileMirror    string
)

var compileCmd = &cobra.Command{
	Use:   "compile <file-or-dir>...",
	Short: "Validate pipeline definitions and compile them into versioned specs",
	Long: `Compile parses pipeline YAML (apiVersion ` + spec.APIVersion + `), validates it, and
writes one canonical JSON spec per pipeline to <out>/<tenant>/<pipeline>/<hash>.json. The file's SHA-256 is its version ID.

Directories are searched recursively for *.yaml and *.yml. If any file has an error,
nothing is written.`,
	Args:         cobra.MinimumNArgs(1),
	SilenceUsage: true,
	RunE:         runCompile,
}

func init() {
	compileCmd.Flags().StringVar(&compileOut, "out", "", "directory to write compiled specs to")
	compileCmd.Flags().BoolVar(&compileCheck, "check", false, "validate only; write nothing (CI lint mode)")
	compileCmd.Flags().BoolVar(&compileNoResolve, "no-resolve", false, "keep image tags instead of resolving them to digests")
	compileCmd.Flags().StringVar(&compileMirror, "registry-mirror", "", "resolve image digests against this registry instead (air-gapped installs)")
	rootCmd.AddCommand(compileCmd)
}

func runCompile(cmd *cobra.Command, args []string) error {
	if compileCheck && compileOut != "" {
		return fmt.Errorf("--check and --out are mutually exclusive")
	}

	files, err := collectYAML(args)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no *.yaml or *.yml files found in %s", strings.Join(args, ", "))
	}

	opts := spec.Options{}
	if !compileNoResolve {
		opts.Resolver = spec.RegistryResolver{Mirror: compileMirror}
	}

	var (
		compiled []spec.Compiled
		failed   int
		seen     = map[string]string{} // tenant/name → first file
	)
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		results, err := spec.CompileFile(cmd.Context(), f, data, opts)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			failed++
			continue
		}
		for _, r := range results {
			key := r.Spec.Tenant + "/" + r.Spec.Name
			if prev, dup := seen[key]; dup {
				fmt.Fprintf(os.Stderr, "%s: pipeline %s is also defined in %s\n", f, key, prev)
				failed++
				continue
			}
			seen[key] = f
			compiled = append(compiled, r)
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d file(s) with errors; nothing written", failed)
	}

	for _, c := range compiled {
		line := fmt.Sprintf("✓ %-40s %s  %s", c.Spec.Tenant+"/"+c.Spec.Name, c.Hash[:12], c.Path)
		if compileOut != "" {
			dest, err := writeSpec(compileOut, c)
			if err != nil {
				return err
			}
			line += "  → " + dest
		}
		fmt.Println(line)
	}
	return nil
}

func collectYAML(args []string) ([]string, error) {
	var files []string
	for _, a := range args {
		info, err := os.Stat(a)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			files = append(files, a)
			continue
		}
		err = filepath.WalkDir(a, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && (strings.HasSuffix(p, ".yaml") || strings.HasSuffix(p, ".yml")) {
				files = append(files, p)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(files)
	return files, nil
}

// writeSpec writes the canonical bytes, so sha256(file) equals the version ID.
func writeSpec(out string, c spec.Compiled) (string, error) {
	dir := filepath.Join(out, c.Spec.Tenant, c.Spec.Name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	dest := filepath.Join(dir, c.Hash+".json")
	if existing, err := os.ReadFile(dest); err == nil {
		if string(existing) == string(c.Canonical) {
			return dest, nil
		}
		return "", fmt.Errorf("%s exists with different content (hash collision or tampering)", dest)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	tmp := dest + ".tmp"
	if err := os.WriteFile(tmp, c.Canonical, 0o644); err != nil {
		return "", err
	}
	return dest, os.Rename(tmp, dest)
}
