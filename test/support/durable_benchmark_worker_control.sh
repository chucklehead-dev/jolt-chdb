#!/bin/sh
set -eu
test "$#" -eq 4
mode=$1
template=$2
result=$3
marker=$4
case "$mode" in success|nonzero|duplicate|timeout) ;; *) exit 64;; esac
cat "$template" > "$result"
printf '%s\n' "$marker"
case "$mode" in
  nonzero) exit 7;;
  duplicate) printf '%s\n' "$marker";;
  timeout)
    trap 'printf ":worker-control-terminated-zero\n"; exit 0' TERM
    printf ':worker-control-ready\n'
    while :; do sleep 1; done;;
esac
