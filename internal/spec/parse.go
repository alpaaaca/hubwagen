package spec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// Compiled is one pipeline compiled from a source file.
type Compiled struct {
	Path      string
	Spec      *Spec
	Canonical []byte
	Hash      string
}

// FileError attaches the source file (and document index, for multi-document
// files) to compilation errors.
type FileError struct {
	Path     string
	Doc      int // 0-based; only shown when the file has several documents
	Err      error
	multiDoc bool
}

func (e *FileError) Error() string {
	where := e.Path
	if e.multiDoc {
		where = fmt.Sprintf("%s (document %d)", e.Path, e.Doc+1)
	}
	var fes Errors
	if errors.As(e.Err, &fes) {
		var buf bytes.Buffer
		for i, fe := range fes {
			if i > 0 {
				buf.WriteByte('\n')
			}
			fmt.Fprintf(&buf, "%s: %s", where, fe.Error())
		}
		return buf.String()
	}
	return where + ": " + e.Err.Error()
}

func (e *FileError) Unwrap() error { return e.Err }

// CompileFile parses every YAML document in data and compiles each one.
func CompileFile(ctx context.Context, path string, data []byte, opts Options) ([]Compiled, error) {
	docs, err := splitDocuments(data)
	if err != nil {
		return nil, &FileError{Path: path, Err: err}
	}
	if len(docs) == 0 {
		return nil, &FileError{Path: path, Err: fmt.Errorf("no YAML documents")}
	}

	var out []Compiled
	var errs []error
	for i, node := range docs {
		fail := func(err error) {
			errs = append(errs, &FileError{Path: path, Doc: i, Err: err, multiDoc: len(docs) > 1})
		}

		doc, err := decodeDocument(node)
		if err != nil {
			fail(err)
			continue
		}
		s, err := Compile(ctx, doc, opts)
		if err != nil {
			fail(err)
			continue
		}
		canon, err := s.Canonical()
		if err != nil {
			fail(err)
			continue
		}
		out = append(out, Compiled{Path: path, Spec: s, Canonical: canon, Hash: HashBytes(canon)})
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return out, nil
}

func splitDocuments(data []byte) ([]*yaml.Node, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var docs []*yaml.Node
	for {
		var n yaml.Node
		err := dec.Decode(&n)
		if errors.Is(err, io.EOF) {
			return docs, nil
		}
		if err != nil {
			return nil, err
		}
		docs = append(docs, &n)
	}
}

func decodeDocument(node *yaml.Node) (*Document, error) {
	var probe map[string]any
	if err := node.Decode(&probe); err != nil {
		return nil, fmt.Errorf("document must be a YAML mapping: %w", err)
	}
	if _, ok := probe["apiVersion"]; !ok {
		return nil, fmt.Errorf("not a hubwagen pipeline: missing apiVersion (expected %q)", APIVersion)
	}

	// Re-encode the node so it can be decoded strictly: yaml.Node.Decode has
	// no KnownFields option, only yaml.Decoder does.
	raw, err := yaml.Marshal(node)
	if err != nil {
		return nil, err
	}
	var d Document
	if err := strictDecode(raw, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func strictDecode(raw []byte, v any) error {
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	return dec.Decode(v)
}
