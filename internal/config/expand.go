package config

import (
	"fmt"
	"regexp"
	"strings"
)

// Variables. A config file may use ${NAME} and ${NAME:-default} for values
// that differ per machine (a bucket, a port, a path): they are replaced
// with the environment's values before the file is parsed. ${NAME} must be
// set (it may be empty); the default applies when NAME is unset or empty.
// $${ stands for a literal ${, and a bare $NAME is left alone, so worker
// commands like [sh, -c, "echo $HOME"] mean what they say. Lines that are
// only a comment are left as they are.
//
// The replacement is textual, so a line keeps its number and errors their
// locations. A value is inserted as is: quote the field ("${NAME}") if it
// may hold YAML characters such as ": " or " #". A value can't hold a line
// break.

var variable = regexp.MustCompile(`\$\$\{|\$\{([^}]*)\}|\$\{`)

var varName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// expand replaces the variables in data, looking them up with lookup
// (os.LookupEnv). It reports every problem, by line.
func expand(data []byte, lookup func(string) (string, bool)) ([]byte, error) {
	lines := strings.SplitAfter(string(data), "\n")
	var errs []string
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "#") || !strings.Contains(line, "${") {
			continue
		}
		at := fmt.Sprintf("line %d", i+1)
		lines[i] = variable.ReplaceAllStringFunc(line, func(m string) string {
			if m == "$${" {
				return "${"
			}
			if m == "${" {
				errs = append(errs, fmt.Sprintf("%s: ${ without a closing }", at))
				return m
			}
			expr := m[2 : len(m)-1]
			name, def, hasDef := strings.Cut(expr, ":-")
			if !varName.MatchString(name) {
				errs = append(errs, fmt.Sprintf("%s: %s: want ${NAME} or ${NAME:-default}", at, m))
				return m
			}
			v, ok := lookup(name)
			switch {
			case hasDef && v == "":
				v = def
			case !ok:
				errs = append(errs, fmt.Sprintf("%s: %s is not set (give it a default: ${%s:-…})", at, m, name))
				return m
			}
			if strings.ContainsAny(v, "\r\n") {
				errs = append(errs, fmt.Sprintf("%s: %s: the value has a line break", at, m))
				return m
			}
			return v
		})
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(errs, "\n"))
	}
	return []byte(strings.Join(lines, "")), nil
}
