package spec

// APIVersion and KindPipeline identify the authoring format compiled by this
// package (see specs/003-platform-redesign/contracts/pipeline-spec.md).
const (
	APIVersion   = "hubwagen.dev/v1alpha1"
	KindPipeline = "Pipeline"
)

// Document is a pipeline definition as authored in YAML. It is decoded
// strictly (unknown fields are errors) and never used by the control plane
// directly — Compile turns it into a Spec.
type Document struct {
	APIVersion string   `yaml:"apiVersion"`
	Kind       string   `yaml:"kind"`
	Metadata   Metadata `yaml:"metadata"`
	Spec       Body     `yaml:"spec"`
}

type Metadata struct {
	Name        string            `yaml:"name"`
	Tenant      string            `yaml:"tenant"`
	Description string            `yaml:"description"`
	Labels      map[string]string `yaml:"labels"`
}

type Body struct {
	Schedule    *ScheduleDef        `yaml:"schedule"`
	Triggers    []TriggerDef        `yaml:"triggers"`
	Params      map[string]ParamDef `yaml:"params"`
	Concurrency ConcurrencyDef      `yaml:"concurrency"`
	Defaults    DefaultsDef         `yaml:"defaults"`
	Tasks       []TaskDef           `yaml:"tasks"`
}

type ScheduleDef struct {
	Cron     string `yaml:"cron"`
	Timezone string `yaml:"timezone"`
	Interval string `yaml:"interval"`
	Catchup  *bool  `yaml:"catchup"`
	Start    string `yaml:"start"`
}

type TriggerDef struct {
	Asset     string `yaml:"asset"`
	Partition string `yaml:"partition"`
}

type ParamDef struct {
	Type    string `yaml:"type"`
	Default any    `yaml:"default"`
}

type ConcurrencyDef struct {
	MaxActiveRuns int    `yaml:"max_active_runs"`
	OnOverlap     string `yaml:"on_overlap"`
}

type DefaultsDef struct {
	Retry   *RetryDef   `yaml:"retry"`
	Timeout string      `yaml:"timeout"`
	Runtime *RuntimeDef `yaml:"runtime"`
}

type TaskDef struct {
	ID          string            `yaml:"id"`
	DependsOn   []string          `yaml:"depends_on"`
	TriggerRule string            `yaml:"trigger_rule"`
	OnFailure   string            `yaml:"on_failure"`
	Runtime     *RuntimeDef       `yaml:"runtime"`
	Command     []string          `yaml:"command"`
	Args        []string          `yaml:"args"`
	Env         map[string]string `yaml:"env"`
	Secrets     []SecretDef       `yaml:"secrets"`
	Inputs      []InputDef        `yaml:"inputs"`
	Outputs     []OutputDef       `yaml:"outputs"`
	Retry       *RetryDef         `yaml:"retry"`
	Timeout     string            `yaml:"timeout"`
}

type RuntimeDef struct {
	Kind      string        `yaml:"kind"`
	Pool      string        `yaml:"pool"`
	Image     string        `yaml:"image"`
	Resources *ResourcesDef `yaml:"resources"`
}

type ResourcesDef struct {
	CPU    string `yaml:"cpu"`
	Memory string `yaml:"memory"`
}

type RetryDef struct {
	Limit   *int   `yaml:"limit"`
	Backoff string `yaml:"backoff"`
	Initial string `yaml:"initial"`
	Max     string `yaml:"max"`
}

type SecretDef struct {
	Name string `yaml:"name"`
	Key  string `yaml:"key"`
	Env  string `yaml:"env"`
}

type InputDef struct {
	Name string `yaml:"name"`
	From string `yaml:"from"`
}

type OutputDef struct {
	Name      string `yaml:"name"`
	Asset     string `yaml:"asset"`
	Partition string `yaml:"partition"`
}
