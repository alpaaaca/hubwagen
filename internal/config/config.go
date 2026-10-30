// Package config loads hubwagen.yaml, the instance configuration: where the
// server keeps its data and its bucket, what it listens on, and (in dev) which worker
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

	"github.com/alper/hubwagen/internal/blob"
)

const (
	APIVersion = "hubwagen.dev/v1alpha1"
	Kind       = "Config"
	// FileName is the config file looked for in the current directory.
	FileName = "hubwagen.yaml"

	DefaultDataDir = ".hubwagen"
	DefaultListen  = "127.0.0.1:7070"
)

// Variables hubwagen sets for every worker it starts; a worker's env can't
// set them.
const (
	EnvServer   = "HUBWAGEN_SERVER"
	EnvWorkerID = "HUBWAGEN_WORKER_ID"
	// EnvSupervisorPID is the PID of the hubwagen process that started the
	// worker. A worker should stop once its parent is no longer that
	// process, so a supervisor that dies without stopping its workers (kill
	// -9, a crash) leaves none behind.
	EnvSupervisorPID = "HUBWAGEN_SUPERVISOR_PID"
)

type Config struct {
	APIVersion string   `yaml:"apiVersion"`
	Kind       string   `yaml:"kind"`
	DataDir    string   `yaml:"data_dir"`
	Bucket     Bucket   `yaml:"bucket"`
	Server     Server   `yaml:"server"`
	Workers    []Worker `yaml:"workers"`

	// Path is the file the config came from, "" for the defaults. Relative
	// paths in the file (data_dir, workers[].dir) are resolved against its
	// directory, so Load returns them absolute.
	Path string `yaml:"-"`
}

// Bucket is where pallets (data versions) are kept. Without a URL it is
// file://<data_dir>/bucket. Only the server uses it: workers get URLs from
// the server for what they read and write.
type Bucket struct {
	// URL is s3://<bucket>[/<prefix>]: AWS S3 or an S3-compatible store.
	URL string `yaml:"url"`
	// Endpoint is the store's URL (MinIO, R2, …); empty for AWS.
	Endpoint string `yaml:"endpoint"`
	// PublicEndpoint is the endpoint in the URLs workers get, if they reach
	// the store under another name than the server does.
	PublicEndpoint string `yaml:"public_endpoint"`
	Region         string `yaml:"region"`
	PathStyle      bool   `yaml:"path_style"`
	// AccessKeyIDFile and SecretAccessKeyFile are files holding the
	// credentials; the config never holds secret values. Without them, the
	// usual AWS sources apply (environment, shared credentials file,
	// instance or pod role).
	AccessKeyIDFile     string `yaml:"access_key_id_file"`
	SecretAccessKeyFile string `yaml:"secret_access_key_file"`
	// Transfers is how workers move files: "presigned" (default for s3://)
	// gives them presigned URLs of the store; "server" sends the bytes
	// through the server, for workers that can't reach the store.
	Transfers string `yaml:"transfers"`
}

// Transfer modes.
const (
	TransfersPresigned = "presigned"
	TransfersServer    = "server"
)

// S3 is the s3:// bucket's settings with the credentials read from their
// files, or nil for the file:// bucket under the data dir.
func (b Bucket) S3() (*blob.S3Config, error) {
	if b.URL == "" {
		return nil, nil
	}
	c := &blob.S3Config{URL: b.URL, Endpoint: b.Endpoint, PublicEndpoint: b.PublicEndpoint, Region: b.Region, PathStyle: b.PathStyle}
	var err error
	if c.AccessKeyID, err = secret("bucket.access_key_id_file", b.AccessKeyIDFile); err != nil {
		return nil, err
	}
	if c.SecretAccessKey, err = secret("bucket.secret_access_key_file", b.SecretAccessKeyFile); err != nil {
		return nil, err
	}
	return c, nil
}

// secret reads a credential file; trailing newlines don't count.
func secret(field, path string) (string, error) {
	if path == "" {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("%s: %w", field, err)
	}
	v := strings.TrimRight(string(data), "\r\n")
	if v == "" {
		return "", fmt.Errorf("%s: %s is empty", field, path)
	}
	return v, nil
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
	c.Bucket.validate(add)
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
			if k == EnvServer || k == EnvWorkerID || k == EnvSupervisorPID {
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

func (b Bucket) validate(add func(string, ...any)) {
	switch {
	case b.URL == "":
		if b != (Bucket{}) {
			add("bucket.url: required with the other bucket settings (s3://bucket[/prefix])")
		}
		return
	case !strings.HasPrefix(b.URL, "s3://") || len(b.URL) == len("s3://"):
		add("bucket.url: %q: want s3://bucket[/prefix]; without a url the bucket is <data_dir>/bucket", b.URL)
	}
	for field, e := range map[string]string{"endpoint": b.Endpoint, "public_endpoint": b.PublicEndpoint} {
		if e != "" && !strings.HasPrefix(e, "http://") && !strings.HasPrefix(e, "https://") {
			add("bucket.%s: %q: want http(s)://host[:port]", field, e)
		}
	}
	if (b.AccessKeyIDFile == "") != (b.SecretAccessKeyFile == "") {
		add("bucket: give both access_key_id_file and secret_access_key_file, or neither")
	}
	if b.Transfers != "" && b.Transfers != TransfersPresigned && b.Transfers != TransfersServer {
		add("bucket.transfers: must be %s or %s, got %q", TransfersPresigned, TransfersServer, b.Transfers)
	}
}

func (c *Config) resolve(dir string) {
	if c.DataDir == "" {
		c.DataDir = DefaultDataDir
	}
	if c.Server.Listen == "" {
		c.Server.Listen = DefaultListen
	}
	c.DataDir = abs(dir, c.DataDir)
	for _, p := range []*string{&c.Bucket.AccessKeyIDFile, &c.Bucket.SecretAccessKeyFile} {
		if *p != "" {
			*p = abs(dir, *p)
		}
	}
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
