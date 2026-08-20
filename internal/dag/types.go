package dag

type StepStatus string

const (
	StatusPending   StepStatus = "pending"
	StatusRunning   StepStatus = "running"
	StatusSucceeded StepStatus = "succeeded"
	StatusFailed    StepStatus = "failed"
	StatusSkipped   StepStatus = "skipped"
	StatusCancelled StepStatus = "cancelled"
)

type DAG struct {
	Name        string            `yaml:"name"`
	Description string            `yaml:"description"`
	Schedule    string            `yaml:"schedule"`
	Timezone    string            `yaml:"timezone"`
	Concurrency ConcurrencyConfig `yaml:"concurrency"`
	Env         map[string]string `yaml:"env"`
	Retry       RetryPolicy       `yaml:"retry"`
	Steps       []Step            `yaml:"steps"`
}

type Step struct {
	ID         string            `yaml:"id"`
	Name       string            `yaml:"name"`
	Executor   string            `yaml:"executor"`
	DependsOn  []string          `yaml:"depends_on"`
	Command    string            `yaml:"command"`
	Env        map[string]string `yaml:"env"`
	TimeoutSec int               `yaml:"timeout_sec"`
	Retry      *RetryPolicy      `yaml:"retry"`
	OnFailure  string            `yaml:"on_failure"`
	HTTP       *HTTPConfig       `yaml:"http"`
	SQL        *SQLConfig        `yaml:"sql"`
}

type RetryPolicy struct {
	Limit       int  `yaml:"limit"`
	IntervalSec int  `yaml:"interval_sec"`
	Backoff     bool `yaml:"backoff"`
}

type ConcurrencyConfig struct {
	MaxRuns   int    `yaml:"max_runs"`
	OnOverlap string `yaml:"on_overlap"`
}

type HTTPConfig struct {
	Method  string            `yaml:"method"`
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
	Body    string            `yaml:"body"`
}

type SQLConfig struct {
	DSN   string `yaml:"dsn"`
	Query string `yaml:"query"`
}
