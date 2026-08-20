package dag

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	validNameRe    = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	validExecutors = map[string]bool{"shell": true, "http": true, "sql": true, "noop": true}
	validOnFailure = map[string]bool{"stop": true, "continue": true, "skip_dependents": true}
	validOnOverlap = map[string]bool{"skip": true, "queue": true, "cancel_running": true}
)

func Validate(d *DAG) []error {
	var errs []error

	if d.Name == "" {
		errs = append(errs, fmt.Errorf("dag name is required"))
	} else if !validNameRe.MatchString(d.Name) {
		errs = append(errs, fmt.Errorf("dag name %q must contain only [a-zA-Z0-9_-]", d.Name))
	}

	if len(d.Steps) == 0 {
		errs = append(errs, fmt.Errorf("dag must have at least one step"))
		return errs
	}

	seen := make(map[string]bool, len(d.Steps))
	for i, s := range d.Steps {
		if s.ID == "" {
			errs = append(errs, fmt.Errorf("step[%d]: id is required", i))
			continue
		}
		if seen[s.ID] {
			errs = append(errs, fmt.Errorf("duplicate step id %q", s.ID))
		}
		seen[s.ID] = true
	}

	for _, s := range d.Steps {
		if s.ID == "" {
			continue
		}
		if !validExecutors[s.Executor] {
			errs = append(errs, fmt.Errorf("step %q: unknown executor %q", s.ID, s.Executor))
		}
		if s.Executor == "shell" && strings.TrimSpace(s.Command) == "" {
			errs = append(errs, fmt.Errorf("step %q: shell executor requires a non-empty command", s.ID))
		}
		if s.OnFailure != "" && !validOnFailure[s.OnFailure] {
			errs = append(errs, fmt.Errorf("step %q: on_failure %q must be stop|continue|skip_dependents", s.ID, s.OnFailure))
		}
		for _, dep := range s.DependsOn {
			if !seen[dep] {
				errs = append(errs, fmt.Errorf("step %q: depends_on references unknown step %q", s.ID, dep))
			}
		}
	}

	if d.Concurrency.OnOverlap != "" && !validOnOverlap[d.Concurrency.OnOverlap] {
		errs = append(errs, fmt.Errorf("concurrency.on_overlap %q must be skip|queue|cancel_running", d.Concurrency.OnOverlap))
	}

	// Only run cycle check when all depends_on references are resolved;
	// an unresolved reference causes a false cycle error in TopologicalWaves.
	if len(errs) == 0 {
		if _, err := TopologicalWaves(d.Steps); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}
