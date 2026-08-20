package dag

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ParseFile reads and unmarshals a DAG YAML file. Template tokens ({{ .Field }})
// are intentionally left un-rendered here; interpolation happens at run time in runner.go.
func ParseFile(path string) (*DAG, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading DAG file: %w", err)
	}
	var d DAG
	if err := yaml.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("parsing DAG YAML: %w", err)
	}
	return &d, nil
}
