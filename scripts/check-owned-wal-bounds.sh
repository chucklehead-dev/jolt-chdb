#!/usr/bin/env bash
set -euo pipefail
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$repo_root"
command -v z3 >/dev/null
sha256sum --check formal/smt/owned-wal-bounds/source-sha256.txt
for item in access-bounds:unsat loop-induction:unsat reservation-mutant:sat small-control:unsat inclusive-boundary:sat; do
  name=${item%:*}
  expected=${item#*:}
  actual=$({ sed -n '1,160p' "formal/smt/owned-wal-bounds/$name.smt2"; printf '\n(check-sat)\n'; } | z3 -in -smt2)
  if [[ "$actual" != "$expected" ]]; then
    printf 'FAIL %s: expected %s, got %s\n' "$name" "$expected" "$actual" >&2
    exit 1
  fi
  printf 'OK %s: %s\n' "$name" "$actual"
done
python3 - <<'PY'
from pathlib import Path
import subprocess

source = Path('formal/smt/owned-wal-bounds/loop-induction.smt2').read_text()
small = ('\n(assert (= action 1))\n(assert (= n 1))\n(assert (= i 0))\n'
         '(assert (= at 8))\n(assert (= total 0))\n(assert (= width 1))\n')
cases = {
    'induction-step-mutant': (source.replace(
        '(ite (= action 1) (+ at width) 0)',
        '(ite (= action 1) (+ at 536870911) 0)') + small, 'sat'),
    'induction-small': (source + small, 'unsat'),
    'induction-boundary': (source.replace(
        '(assert (! violation :named counterexample))',
        '(assert (! (not violation) :named valid_boundary))') +
        '\n(assert (= action 1))\n(assert (= n 101))\n(assert (= i 100))\n'
        '(assert (= at 244))\n(assert (= total 0))\n(assert (= width 6))\n', 'sat'),
}
for name, (query, expected) in cases.items():
    result = subprocess.run(['z3', '-in', '-smt2'], input=query + '\n(check-sat)\n',
                            text=True, capture_output=True, check=True)
    actual = result.stdout.strip()
    if actual != expected:
        raise SystemExit(f'FAIL {name}: expected {expected}, got {actual}')
    print(f'OK {name}: {actual}')
PY
