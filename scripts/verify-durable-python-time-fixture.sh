#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture="$repo_root/test/fixtures/durable/python-decimal-time-oracle.json"
generated=$(mktemp)
trap 'rm -f "$generated"' EXIT

python3 "$repo_root/scripts/generate-durable-python-time-fixture.py" > "$generated"
if ! cmp -s "$fixture" "$generated"; then
  echo "Durable Python Decimal time fixture drifted" >&2
  diff -u "$fixture" "$generated" >&2 || true
  exit 1
fi

echo "verified Durable Python Decimal time fixture"
