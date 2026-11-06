// Package config loads moksha.yaml, the instance configuration: where the
// server keeps its data and its bucket, what it listens on, and (in dev) which worker
// processes to start next to it.
//
// The file is decoded strictly: unknown fields, wrong types and duplicate
// keys are errors, and every problem is reported at once with its location.
// Values may come from the environment: ${NAME}, ${NAME:-default}.
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
	"time"

	"gopkg.in/yaml.v3"

	"github.com/alper/moksha/internal/blob"
)

const (
	APIVersion = "moksha.dev/v1alpha1"
	Kind       = "Config"
	// FileName is the config file looked for in the current directory.
	FileName = "moksha.yaml"

	DefaultDataDir = ".moksha"
	DefaultListen  = "127.0.0.1:7070"
)

// Variables moksha sets for every worker it starts; a worker's env can't
// set them.
const (
	EnvServer   = "MOKSHA_SERVER"
	EnvWorkerID = "MOKSHA_WORKER_ID"
	// EnvSupervisorPID is the PID of the moksha process that started the
	// worker. A worker should stop once its parent is no longer that
	// process, so a supervisor that dies without stopping its workers (kill
	// -9, a crash) leaves none behind.
	EnvSupervisorPID = "MOKSHA_SUPERVISOR_PID"
	// EnvQueues and EnvSteps are the group's queues (comma-separated) and
	// steps per worker: the Python worker's defaults for --queue and --steps.
	EnvQueues = "MOKSHA_QUEUES"
	EnvSteps  = "MOKSHA_STEPS"
)

type Config struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	DataDir    string `yaml:"data_dir"`
	Bucket     Bucket `yaml:"bucket"`
	Server     Server `yaml:"server"`
	// Pools are worker pools the server scales but doesn't start
	// (launcher: external). Pools of a project's own workers
	// come from its project file (docs/projects.md).
	Pools []Pool `yaml:"pools"`
	// OldWorkers is the workers: section of configs before pools:, kept
	// only to say what to write instead.
	OldWorkers yaml.Node `yaml:"workers"`
	// Kubernetes is how the server reaches the cluster that runs
	// deployed projects with projects.launcher: kubernetes.
	Kubernetes Kubernetes `yaml:"kubernetes"`
	// Projects is how the server runs deployed projects' workers
	// (docs/projects.md): containers of their image with Runtime, reaching
	// the API at ServerURL (defaults as for launcher: container).
	Projects Projects `yaml:"projects"`

	// Path is the file the config came from, "" for the defaults. Relative
	// paths in the file (data_dir, the bucket's and projects' files) are resolved against its
	// directory, so Load returns them absolute.
	Path string `yaml:"-"`
}

// Bucket is where data versions (data versions) are kept. Without a URL it is
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

// Projects is how deployed projects' workers run.
type Projects struct {
	// Launcher: "" runs a deployment's workers on this machine (containers
	// of its image, or processes in its directory); "kubernetes" runs each
	// pool as a Deployment in the kubernetes section's cluster, which needs
	// an image and ServerURL.
	Launcher  string `yaml:"launcher"`
	Runtime   string `yaml:"runtime"`
	ServerURL string `yaml:"server_url"`
	// AuthFile: the registry credentials the runtime pulls their images
	// with (podman --authfile; Docker: a config.json, as DOCKER_CONFIG).
	AuthFile string `yaml:"auth_file"`
	// SecretsDir: a file per secret ({secret: name} in a project file),
	// read when a worker starts.
	SecretsDir string `yaml:"secrets_dir"`
}

// Kubernetes is the cluster for launcher: kubernetes pools. Without APIURL
// the server uses its in-cluster service account; with one (kubectl proxy:
// http://127.0.0.1:8001) it sends no credentials.
type Kubernetes struct {
	APIURL    string `yaml:"api_url"`
	Namespace string `yaml:"namespace"` // default: the service account's, else default
	// Instance labels the Deployments this server makes (moksha.dev/instance),
	// so it removes only its own; default moksha. Two servers in one
	// namespace need different ones.
	Instance string `yaml:"instance"`
	// ImagePullSecrets are Secrets for pulling projects' images, named in
	// their pods.
	ImagePullSecrets []string `yaml:"image_pull_secrets"`
}

// DefaultInstance is kubernetes.instance's default.
const DefaultInstance = "moksha"

// InstanceName is the server's kubernetes.instance.
func (k Kubernetes) InstanceName() string {
	if k.Instance == "" {
		return DefaultInstance
	}
	return k.Instance
}

type Server struct {
	Listen string `yaml:"listen"`
}

// Pool is a worker pool in the server's config: between min and max
// workers, a count the server works out from the work waiting on its
// queues (internal/pools), and that something else applies (KEDA, an
// HPA, from GET /v1/pools/<name>).
type Pool struct {
	Name     string   `yaml:"name"`
	Launcher string   `yaml:"launcher"` // external
	Min      *int     `yaml:"min"`      // default 0
	Max      *int     `yaml:"max"`      // default max(min, 1)
	Queues   []string `yaml:"queues"`   // default [default]
	Steps    *int     `yaml:"steps"`    // steps one worker runs at once; default 4
	// IdleAfter: a worker above min that had nothing to run for this long
	// isn't counted. Default DefaultIdleAfter.
	IdleAfter Duration `yaml:"idle_after"`
}

// Bounds is the pool's min and max.
func (p Pool) Bounds() (lo, hi int) {
	if p.Min != nil {
		lo = *p.Min
	}
	hi = max(lo, 1)
	if p.Max != nil {
		hi = *p.Max
	}
	return lo, hi
}

// QueueList is the queues the pool's workers poll.
func (p Pool) QueueList() []string {
	if len(p.Queues) == 0 {
		return []string{"default"}
	}
	return p.Queues
}

// StepCount is how many steps one worker runs at once.
func (p Pool) StepCount() int {
	if p.Steps == nil {
		return DefaultSteps
	}
	return *p.Steps
}

// Idle is how long a worker above min may have nothing to run.
func (p Pool) Idle() time.Duration {
	if p.IdleAfter == 0 {
		return DefaultIdleAfter
	}
	return time.Duration(p.IdleAfter)
}

// Worker is how a deployed pool's workers start (cmd, internal/launch):
// processes of Command in Dir, or containers of Image. Each gets
// MOKSHA_SERVER (the API's URL), MOKSHA_WORKER_ID (<name>-<n>),
// MOKSHA_QUEUES and MOKSHA_STEPS. Not in any file: the server makes it
// from a project's deployment.
type Worker struct {
	Name string
	// Launcher is LauncherProcess (Command in Dir) or LauncherContainer
	// (Image, with Runtime: docker or podman, default the one on PATH,
	// reaching the API at ServerURL, default the listen address with
	// host.docker.internal or host.containers.internal for a loopback
	// host, pulling with AuthFile's credentials). Command, if set,
	// replaces the image's.
	Launcher  string
	Image     string
	Runtime   string
	ServerURL string
	AuthFile  string
	Command   []string
	Queues    []string // default [default]
	Steps     *int     // steps one worker runs at once; default 4
	Dir       string
	Env       map[string]string
}

const (
	DefaultSteps     = 4 // the Python worker's default
	DefaultIdleAfter = 5 * time.Minute
)

// Launcher values.
const (
	LauncherProcess    = "process"
	LauncherExternal   = "external"
	LauncherContainer  = "container"
	LauncherKubernetes = "kubernetes"
)

// QueueList is the queues the workers poll.
func (w Worker) QueueList() []string {
	if len(w.Queues) == 0 {
		return []string{"default"}
	}
	return w.Queues
}

// StepCount is how many steps one worker runs at once.
func (w Worker) StepCount() int {
	if w.Steps == nil {
		return DefaultSteps
	}
	return *w.Steps
}

// Duration is a time.Duration written as in Go ("5m", "90s").
type Duration time.Duration

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %q: want a duration such as 5m or 90s", n.Line, n.Value)
	}
	*d = Duration(v)
	return nil
}

// Find picks the config file: explicit (--config) if set, else
// $MOKSHA_CONFIG, else ./moksha.yaml if it exists. "" means none: use
// the defaults.
func Find(explicit string) (string, error) {
	for _, p := range []string{explicit, os.Getenv("MOKSHA_CONFIG")} {
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
		return Defaults(wd), nil
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

// Defaults is the config without a file, its paths relative to dir: for
// moksha dev in a project's directory, the data in dir/.moksha.
func Defaults(dir string) *Config {
	c := &Config{APIVersion: APIVersion, Kind: Kind}
	c.resolve(dir)
	return c
}

var labelValue = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?$`)

var workerName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// Parse decodes and validates a config file, after replacing its
// variables from the environment (expand.go); relative paths are resolved
// against dir.
func Parse(data []byte, dir string) (*Config, error) {
	data, err := expand(data, os.LookupEnv)
	if err != nil {
		return nil, err
	}
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
	if r := c.Projects.Runtime; r != "" && r != "docker" && r != "podman" {
		add("projects.runtime: must be docker or podman, got %q", r)
	}
	switch c.Projects.Launcher {
	case "":
	case LauncherKubernetes:
		if c.Projects.ServerURL == "" {
			add("projects.server_url: required with projects.launcher: kubernetes (the server's Service as pods reach it, e.g. http://moksha.moksha.svc:7070)")
		}
	default:
		add("projects.launcher: must be kubernetes, or none for this machine, got %q", c.Projects.Launcher)
	}
	if i := c.Kubernetes.Instance; i != "" && !labelValue.MatchString(i) {
		add("kubernetes.instance: %q: at most 63 letters, digits, '-', '_' and '.', starting and ending with a letter or digit", i)
	}
	if c.OldWorkers.Kind != 0 {
		add("line %d: workers: is gone (docs/projects.md). A project's workers come from its project file, "+
			"a moksha.yaml with kind: Project and pools: [{name: default, min: 1, max: 3}]: moksha dev in its directory "+
			"deploys it, moksha deploy hands it to a server. Pools started elsewhere (launcher: external or kubernetes) "+
			"go under pools: here", c.OldWorkers.Line)
	}
	seen := map[string]bool{}
	for i, p := range c.Pools {
		at := fmt.Sprintf("pools[%d]", i)
		switch {
		case p.Name == "":
			add("%s.name: required", at)
		case !workerName.MatchString(p.Name):
			add("%s.name: %q must be lower-case letters, digits, - and _", at, p.Name)
		case seen[p.Name]:
			add("%s.name: %q is used twice", at, p.Name)
		}
		seen[p.Name] = true
		switch p.Launcher {
		case LauncherExternal:
		case "", LauncherProcess, LauncherContainer, LauncherKubernetes:
			add("%s.launcher: must be external; pools of processes, containers or Deployments come from a project's file "+
				"(kind: Project, docs/projects.md), on Kubernetes with projects.launcher: kubernetes", at)
		default:
			add("%s.launcher: must be external, got %q", at, p.Launcher)
		}
		lo, hi := p.Bounds()
		switch {
		case lo < 0 || hi < 0:
			add("%s: min and max must be 0 or more", at)
		case lo > hi:
			add("%s: min %d is more than max %d", at, lo, hi)
		}
		if p.StepCount() < 1 {
			add("%s.steps: must be 1 or more", at)
		}
		if p.IdleAfter < 0 {
			add("%s.idle_after: must be positive", at)
		}
		for j, d := range p.Queues {
			if d == "" {
				add("%s.queues[%d]: empty", at, j)
			}
		}
	}
	c.resolve(dir)
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
	for _, p := range []*string{&c.Projects.AuthFile, &c.Projects.SecretsDir} {
		if *p != "" {
			*p = abs(dir, *p)
		}
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
