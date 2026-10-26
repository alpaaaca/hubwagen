// Package api serves hubwagen's Connect services over HTTP. The service
// definitions are in proto/; the generated code is in gen/ and committed.
package api

//go:generate go run github.com/bufbuild/buf/cmd/buf@v1.73.0 lint ../../proto
//go:generate go run github.com/bufbuild/buf/cmd/buf@v1.73.0 generate ../../proto --template ../../proto/buf.gen.yaml
