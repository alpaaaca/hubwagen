// Package config loads hubwagen.yaml, the instance configuration: where the
// server keeps its data, what it listens on, and (in dev) which worker
// processes to start next to it.
//
// The file is decoded strictly: unknown fields, wrong types and duplicate
// keys are errors, and every problem is reported at once with its location.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	APIVersion = "hubwagen.dev/v1alpha1"
	Kind       = "Config"
	// FileName is the config file looked for in the current directory.
	FileName = "hubwagen.yaml"

	DefaultDataDir = ".hubwagen"
	DefaultListen  = "127.0.0.1:7070"
)

// Variables the server sets for every worker it starts; a worker's env can't
// set them.
const (
	EnvServer   = "HUBWAGEN_SERVER"
	EnvWorkerID = "HUBWAGEN_WORKER_ID"
)

type Config struct {
	APIVersion string   `yaml:"apiVersion"`
	Kind       string   `yaml:"kind"`
	DataDir    string   `yaml:"data_dir"`
	Server     Server   `yaml:"server"`
	Workers    []Worker `yaml:"workers"`

	// Path is the file the config came from, "" for the defaults. Relative
	// paths in the file (data_dir, workers[].dir) are resolved against its
	// directory, so Load returns them absolute.
	Path string `yaml:"-"`
}

type Server struct {
	Listen string `yaml:"listen"`
}

// Worker is a group of identical worker processes. Each replica gets
// HUBWAGEN_SERVER (the API's URL) and HUBWAGEN_WORKER_ID (<name>-<n>).
type Worker struct {
	Name     string            `yaml:"name"`
	Command  []string          `yaml:"command"`
	Replicas *int              `yaml:"replicas"` // default 1; 0 turns the group off
	Dir      string            `yaml:"dir"`      // working directory; default: the config file's
	Env      map[string]string `yaml:"env"`
}

// Count is the number of replicas.
func (w Worker) Count() int {
	if w.Replicas == nil {
		return 1
	}
	return *w.Replicas
}

// Find picks the config file: explicit (--config) if set, else
// $HUBWAGEN_CONFIG, else ./hubwagen.yaml if it exists. "" means none: use
// the defaults.
func Find(explicit string) (string, error) {
	for _, p := range []string{explicit, os.Getenv("HUBWAGEN_CONFIG")} {
		if p == "" {
			continue
		}
		if _, err := os.Stat(p); err != nil {
			return "", fmt.Errorf("config: %w", err)
		}
		return p, nil
	}
	if _, err := os.Stat(FileName); err == nil {
		return FileName, nil
	}
	return "", nil
}

// Load reads path, or returns the defaults (relative to the current
// directory) if path is "".
func Load(path string) (*Config, error) {
	if path == "" {
		wd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		c := &Config{APIVersion: APIVersion, Kind: Kind}
		c.resolve(wd)
		return c, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	c, err := Parse(data, filepath.Dir(abs))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	c.Path = abs
	return c, nil
}

var workerName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// Parse decodes and validates a config file; relative paths are resolved
// against dir.
func Parse(data []byte, dir string) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	var errs []string
	add := func(format string, args ...any) { errs = append(errs, fmt.Sprintf(format, args...)) }
	if c.APIVersion != APIVersion {
		add("apiVersion: must be %s, got %q", APIVersion, c.APIVersion)
	}
	if c.Kind != Kind {
		add("kind: must be %s, got %q", Kind, c.Kind)
	}
	if c.Server.Listen != "" {
		if _, _, err := net.SplitHostPort(c.Server.Listen); err != nil {
			add("server.listen: %v", err)
		}
	}
	seen := map[string]bool{}
	for i, w := range c.Workers {
		at := fmt.Sprintf("workers[%d]", i)
		switch {
		case w.Name == "":
			add("%s.name: required", at)
		case !workerName.MatchString(w.Name):
			add("%s.name: %q must be lower-case letters, digits, - and _", at, w.Name)
		case seen[w.Name]:
			add("%s.name: %q is used twice", at, w.Name)
		}
		seen[w.Name] = true
		if len(w.Command) == 0 || w.Command[0] == "" {
			add("%s.command: required, e.g. [python3, -m, hubwagen.worker, app.hauls]", at)
		}
		if w.Count() < 0 {
			add("%s.replicas: must be 0 or more", at)
		}
		for k := range w.Env {
			if k == EnvServer || k == EnvWorkerID {
				add("%s.env.%s: set by hubwagen", at, k)
			}
		}
	}
	c.resolve(dir)
	for i, w := range c.Workers {
		if st, err := os.Stat(w.Dir); err != nil || !st.IsDir() {
			add("workers[%d].dir: %s is not a directory", i, w.Dir)
		}
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "\n"))
	}
	return &c, nil
}

func (c *Config) resolve(dir string) {
	if c.DataDir == "" {
		c.DataDir = DefaultDataDir
	}
	if c.Server.Listen == "" {
		c.Server.Listen = DefaultListen
	}
	c.DataDir = abs(dir, c.DataDir)
	for i := range c.Workers {
		c.Workers[i].Dir = abs(dir, c.Workers[i].Dir)
	}
}

func abs(dir, p string) string {
	if filepath.IsAbs(p) {
		return filepath.Clean(p)
	}
	return filepath.Join(dir, p)
}

// ServerURL is the URL workers on this machine use to reach a server
// listening on listen: an unspecified host (0.0.0.0, ::, "") becomes
// 127.0.0.1.
func ServerURL(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "http://" + listen
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}
