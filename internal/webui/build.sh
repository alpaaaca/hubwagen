#!/bin/sh
# Builds ui/ and copies it into dist/, which the binary embeds. Run with
# `go generate ./internal/webui` (needs Node and npm); dist/ isn't committed.
set -e
here=$(cd "$(dirname "$0")" && pwd)
cd "$here/../../ui"
npm ci --no-audit --no-fund
npm run build
find "$here/dist" -mindepth 1 ! -name .gitkeep -exec rm -rf {} +
cp -R dist/. "$here/dist/"
