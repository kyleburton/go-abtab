#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

go install ./cmd/...

BIN_DIR="$(go env GOBIN)"
if [ -z "$BIN_DIR" ]; then
  BIN_DIR="$(go env GOPATH)/bin"
fi

echo "Installed to $BIN_DIR:"
for d in cmd/*/; do
  echo "  $(basename "$d")"
done
echo "Make sure $BIN_DIR is on your PATH."
