#!/bin/sh
set -eu

classpath_file=$1
providers_file=$2

set -- db/datasource.clj \
  db/jdbc.clj \
  db/jdbc_shim.clj \
  db/pg.clj \
  db/sqlite.clj \
  next/jdbc.clj \
  next/jdbc/prepare.clj \
  next/jdbc/result_set.clj \
  next/jdbc/sql.clj \
  next/jdbc/transaction.clj

: > "$providers_file"
failures=0
classpath=$(cat "$classpath_file")
for namespace do
  count=0
  old_ifs=$IFS
  IFS=:
  for root in $classpath; do
    if [ -f "$root/$namespace" ]; then
      count=$((count + 1))
      printf '%s\n' "$root" >> "$providers_file"
    fi
  done
  IFS=$old_ifs
  if [ "$count" -ne 1 ]; then
    echo "provider cardinality violation: $namespace has $count source providers" >&2
    failures=$((failures + 1))
  fi
done

provider_count=$(sort -u "$providers_file" | wc -l | tr -d ' ')
if [ "$provider_count" -ne 1 ]; then
  echo "physical database provider violation: found $provider_count source roots" >&2
  failures=$((failures + 1))
fi

[ "$failures" -eq 0 ]
