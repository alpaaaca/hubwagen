package spec

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/template"
	"text/template/parse"
	"time"
)

// Templates in task args, env values and partition expressions use Go
// text/template syntax, restricted to the variables in Context and the
// functions in templateFuncs. Compile checks every template's parse tree
// against that allow-list (checkTemplate), so the control plane only ever
// renders data into strings — it never evaluates anything a pipeline author
// could turn into code.

// Context is the data a template is rendered against at dispatch time.
type Context struct {
	Interval IntervalVars
	Run      RunVars
	Pipeline string
	Tenant   string
	Params   map[string]any
	Inputs   map[string]string
	Outputs  map[string]string
	Asset    AssetVars
}

type IntervalVars struct {
	Start string // RFC 3339 in the pipeline timezone; empty for ad-hoc runs
	End   string
}

type RunVars struct {
	ID         string
	Key        string
	Generation int
	Date       string // YYYY-MM-DD the run was created, in the pipeline timezone
}

type AssetVars struct {
	Name      string
	Partition string
}

// allowedFields lists the second-level fields of each struct-valued variable.
// Params, Inputs and Outputs are checked against the declared names instead.
var allowedFields = map[string]map[string]bool{
	"Interval": {"Start": true, "End": true},
	"Run":      {"ID": true, "Key": true, "Generation": true, "Date": true},
	"Asset":    {"Name": true, "Partition": true},
}

// Built-in text/template functions that only compare or combine values.
// Notably absent: call, index, slice, html, js, urlquery.
var allowedBuiltins = map[string]bool{
	"eq": true, "ne": true, "lt": true, "le": true, "gt": true, "ge": true,
	"and": true, "or": true, "not": true, "len": true, "printf": true,
}

var templateFuncs = template.FuncMap{
	// date formats an RFC 3339 timestamp: {{ .Interval.Start | date "2006-01-02" }}
	"date": func(layout, value string) (string, error) {
		if value == "" {
			return "", fmt.Errorf("date: empty timestamp (the run has no data interval)")
		}
		t, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return "", fmt.Errorf("date: %w", err)
		}
		return t.Format(layout), nil
	},
	"quote": func(v any) string { return strconv.Quote(fmt.Sprint(v)) },
	// default returns value unless it is empty: {{ default "x" .Params.mode }}
	"default": func(def, value any) any {
		if value == nil || value == "" {
			return def
		}
		return value
	},
	"lower": strings.ToLower,
	"upper": strings.ToUpper,
}

// scope says which variables a particular template may reference.
type scope struct {
	params  map[string]bool
	inputs  map[string]bool
	outputs map[string]bool
	asset   bool // .Asset.* (only in asset-triggered pipelines)
	// assetOnly restricts the template to .Asset.* (trigger partitions are
	// evaluated before a run exists).
	assetOnly bool
}

func parseTemplate(text string) (*template.Template, error) {
	return template.New("").Funcs(templateFuncs).Option("missingkey=error").Parse(text)
}

// checkTemplate parses text and verifies it uses only allowed constructs.
func checkTemplate(text string, sc scope) error {
	t, err := parseTemplate(text)
	if err != nil {
		return err
	}
	if len(t.Templates()) > 1 {
		return fmt.Errorf("template definitions ({{ define }} / {{ block }}) are not allowed")
	}
	if t.Tree == nil || t.Tree.Root == nil {
		return nil
	}
	return checkNode(t.Tree.Root, sc)
}

func checkNode(n parse.Node, sc scope) error {
	switch x := n.(type) {
	case *parse.ListNode:
		if x == nil {
			return nil
		}
		for _, c := range x.Nodes {
			if err := checkNode(c, sc); err != nil {
				return err
			}
		}
	case *parse.TextNode, *parse.CommentNode,
		*parse.StringNode, *parse.NumberNode, *parse.BoolNode, *parse.NilNode:
		return nil
	case *parse.ActionNode:
		return checkNode(x.Pipe, sc)
	case *parse.IfNode:
		if err := checkNode(x.Pipe, sc); err != nil {
			return err
		}
		if err := checkNode(x.List, sc); err != nil {
			return err
		}
		return checkNode(x.ElseList, sc)
	case *parse.PipeNode:
		if x == nil {
			return nil
		}
		if len(x.Decl) > 0 {
			return fmt.Errorf("variable declarations are not allowed")
		}
		for _, c := range x.Cmds {
			if err := checkNode(c, sc); err != nil {
				return err
			}
		}
	case *parse.CommandNode:
		for _, a := range x.Args {
			if err := checkNode(a, sc); err != nil {
				return err
			}
		}
	case *parse.IdentifierNode:
		if _, ok := templateFuncs[x.Ident]; ok || allowedBuiltins[x.Ident] {
			return nil
		}
		return fmt.Errorf("function %q is not allowed", x.Ident)
	case *parse.FieldNode:
		return checkField(x.Ident, sc)
	case *parse.DotNode:
		return fmt.Errorf("bare {{ . }} is not allowed; reference a specific variable")
	case *parse.VariableNode:
		return fmt.Errorf("template variables ($%s) are not allowed", strings.TrimPrefix(x.Ident[0], "$"))
	case *parse.RangeNode:
		return fmt.Errorf("{{ range }} is not allowed")
	case *parse.WithNode:
		return fmt.Errorf("{{ with }} is not allowed")
	case *parse.TemplateNode:
		return fmt.Errorf("{{ template }} is not allowed")
	default:
		return fmt.Errorf("template construct %q is not allowed", n.String())
	}
	return nil
}

func checkField(ident []string, sc scope) error {
	path := "." + strings.Join(ident, ".")
	top := ident[0]

	if sc.assetOnly && top != "Asset" {
		return fmt.Errorf("%s: only .Asset.Name and .Asset.Partition are available here", path)
	}

	switch top {
	case "Pipeline", "Tenant":
		if len(ident) != 1 {
			return fmt.Errorf("%s: unknown variable", path)
		}
		return nil
	case "Interval", "Run", "Asset":
		if top == "Asset" && !sc.asset {
			return fmt.Errorf("%s: .Asset is only available in pipelines with asset triggers", path)
		}
		if len(ident) != 2 || !allowedFields[top][ident[1]] {
			return fmt.Errorf("%s: unknown variable", path)
		}
		return nil
	case "Params", "Inputs", "Outputs":
		declared := map[string]map[string]bool{"Params": sc.params, "Inputs": sc.inputs, "Outputs": sc.outputs}[top]
		if len(ident) != 2 {
			return fmt.Errorf("%s: expected .%s.<name>", path, top)
		}
		if !declared[ident[1]] {
			return fmt.Errorf("%s: %q is not a declared %s", path, ident[1], strings.ToLower(strings.TrimSuffix(top, "s")))
		}
		return nil
	}
	return fmt.Errorf("%s: unknown variable", path)
}

// Render renders a template that passed checkTemplate.
func Render(text string, ctx Context) (string, error) {
	if !strings.Contains(text, "{{") {
		return text, nil
	}
	t, err := parseTemplate(text)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, ctx); err != nil {
		return "", err
	}
	return buf.String(), nil
}
