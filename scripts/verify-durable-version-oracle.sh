#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
corpus="$repo_root/test/fixtures/durable/version-ordering.json"
commit=66643e5030fb73c30ac5cdd31d4c7858ea040ed0
path=chdb/durable/protocol.py
repository=https://github.com/chdb-io/chdb
url="https://raw.githubusercontent.com/chdb-io/chdb/$commit/$path"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

curl -fsS "$url" -o "$scratch/protocol.py"
python3 "$repo_root/scripts/verify-durable-version-oracle.py" \
  "$corpus" "$scratch/protocol.py" "$repository" "$commit" "$path"
