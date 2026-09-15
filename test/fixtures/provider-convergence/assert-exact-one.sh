#!/bin/sh
set -eu

classpath_file=$1
providers_file=$2
reference_root=$3

namespaces_file=$providers_file.namespaces
find "$reference_root/db" "$reference_root/next" -type f -name '*.clj' -print |
  while IFS= read -r source; do
    printf '%s\n' "${source#"$reference_root"/}"
  done |
  LC_ALL=C sort -u > "$namespaces_file"

if [ ! -s "$namespaces_file" ]; then
  echo "provider reference tree contains no db/next.jdbc sources: $reference_root" >&2
  exit 1
fi

: > "$providers_file"
failures=0
classpath=$(cat "$classpath_file")
while IFS= read -r namespace; do
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
done < "$namespaces_file"

provider_count=$(sort -u "$providers_file" | wc -l | tr -d ' ')
if [ "$provider_count" -ne 1 ]; then
  echo "physical database provider violation: found $provider_count source roots" >&2
  failures=$((failures + 1))
fi

[ "$failures" -eq 0 ]
