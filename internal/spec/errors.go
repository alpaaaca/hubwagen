package spec

import (
	"fmt"
	"strings"
)

// FieldError is one validation problem, located by a path into the pipeline
// definition (for example "spec.tasks[1].depends_on[0]").
type FieldError struct {
	Path string
	Msg  string
}

func (e FieldError) Error() string {
	if e.Path == "" {
		return e.Msg
	}
	return e.Path + ": " + e.Msg
}

// Errors collects every problem found in one definition, so authors see all
// of them in a single compile instead of fixing them one at a time.
type Errors []FieldError

func (es Errors) Error() string {
	msgs := make([]string, len(es))
	for i, e := range es {
		msgs[i] = e.Error()
	}
	return strings.Join(msgs, "\n")
}

type collector struct {
	list Errors
}

func (c *collector) add(path, format string, args ...any) {
	c.list = append(c.list, FieldError{Path: path, Msg: fmt.Sprintf(format, args...)})
}

func (c *collector) err() error {
	if len(c.list) == 0 {
		return nil
	}
	return c.list
}
