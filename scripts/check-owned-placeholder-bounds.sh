#!/usr/bin/env bash
set -euo pipefail
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$repo_root"
command -v z3 >/dev/null
sha256sum --check formal/smt/owned-placeholder-bounds/source-sha256.txt
for item in access:unsat induction:unsat lookahead-mutant:sat small-control:unsat inclusive-word:sat; do
  name=${item%:*}
  expected=${item#*:}
  actual=$({ sed -n '1,160p' "formal/smt/owned-placeholder-bounds/$name.smt2"; printf '\n(check-sat)\n'; } | z3 -in -smt2)
  if [[ "$actual" != "$expected" ]]; then
    printf 'FAIL %s: expected %s, got %s\n' "$name" "$expected" "$actual" >&2
    exit 1
  fi
  printf 'OK %s: %s\n' "$name" "$actual"
done
