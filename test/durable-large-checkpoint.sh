#!/usr/bin/env bash
set -euo pipefail

runner=${1:?usage: test/durable-large-checkpoint.sh JOLT-RUNNER}
sizes_mib=(32 64 128)
rss_allowance_kib=98304
rss_plateau_spread_kib=65536
live_growth_allowance_bytes=16777216

if [[ $(basename "$runner") == jolt-with-chez-10.4.1 ]]; then
  run_jolt=("$runner" jolt)
else
  run_jolt=("$runner")
fi

if [[ ! -x /usr/bin/time ]]; then
  echo "FAIL GNU /usr/bin/time is required for isolated-process max RSS" >&2
  exit 2
fi

qualification_root=$(mktemp -d)
fixture_pid=""
cleanup() {
  if [[ -n $fixture_pid ]]; then
    kill "$fixture_pid" 2>/dev/null || true
    wait "$fixture_pid" 2>/dev/null || true
  fi
  rm -rf -- "$qualification_root"
}
trap cleanup EXIT

port_file="$qualification_root/port"
object_root="$qualification_root/objects"
scratch_root="$qualification_root/scratch"
mkdir -p "$object_root" "$scratch_root"

python3 test/support/s3_streaming_fixture.py "$port_file" "$object_root" &
fixture_pid=$!
for _ in $(seq 1 100); do
  [[ -s $port_file ]] && break
  sleep 0.05
done
[[ -s $port_file ]]
endpoint="http://127.0.0.1:$(<"$port_file")"

measure() {
  local mode=$1
  local label=$2
  local source_file=$3
  local output="$qualification_root/$label.out"
  local timing="$qualification_root/$label.time"
  if ! /usr/bin/time -v -o "$timing" \
    "${run_jolt[@]}" -M:durable-large-checkpoint-qualification \
    "$mode" "$endpoint" "$source_file" "$scratch_root" >"$output"; then
    echo "FAIL $label qualification worker failed" >&2
    return 1
  fi
  grep -q '^QUALIFICATION ' "$output"
}

max_rss() {
  awk -F: '/Maximum resident set size/ {
    gsub(/^[[:space:]]+/, "", $2); print $2
  }' "$1"
}

field() {
  local pattern=$1
  local file=$2
  sed -n "s/.*$pattern \([0-9][0-9]*\).*/\1/p" "$file"
}

# Warm the Jolt cache before any measured process so compilation cannot inflate
# only the baseline or only a production-path sample.
"${run_jolt[@]}" -M:durable-large-checkpoint-qualification \
  baseline "$endpoint" unused "$scratch_root" >/dev/null
measure baseline baseline unused
baseline_rss_kib=$(max_rss "$qualification_root/baseline.time")
baseline_record=$(grep '^QUALIFICATION ' "$qualification_root/baseline.out")
stream_limit_kib=$((baseline_rss_kib + rss_allowance_kib))

declare -a records=()
stream_min_rss_kib=0
stream_max_rss_kib=0
total_file_bytes=0

for size_mib in "${sizes_mib[@]}"; do
  source_file="$qualification_root/checkpoint-$size_mib.bin"
  dd if=/dev/zero of="$source_file" bs=1048576 count="$size_mib" status=none

  stream_label="stream-$size_mib"
  retained_label="retained-$size_mib"
  measure stream "$stream_label" "$source_file"
  measure retained-control "$retained_label" "$source_file"

  stream_output="$qualification_root/$stream_label.out"
  retained_output="$qualification_root/$retained_label.out"
  stream_rss_kib=$(max_rss "$qualification_root/$stream_label.time")
  retained_rss_kib=$(max_rss "$qualification_root/$retained_label.time")
  stream_start_live=$(field ':start {:live-bytes' "$stream_output")
  stream_end_live=$(field ':end {:live-bytes' "$stream_output")
  stream_end_reserved=$(field ':end {:live-bytes [0-9][0-9]*, :reserved-bytes' \
                              "$stream_output")
  retained_start_live=$(field ':start {:live-bytes' "$retained_output")
  retained_end_live=$(field ':end {:live-bytes' "$retained_output")
  file_bytes=$((size_mib * 1024 * 1024))
  total_file_bytes=$((total_file_bytes + file_bytes))
  retained_growth_min=$((file_bytes * 3 / 4))
  retained_rss_min=$((baseline_rss_kib + (size_mib * 1024 * 3 / 4)))

  if (( stream_rss_kib > stream_limit_kib )); then
    echo "FAIL $size_mib MiB streamed max RSS exceeded the fixed allowance" >&2
    exit 1
  fi
  if (( stream_end_live - stream_start_live > live_growth_allowance_bytes )); then
    echo "FAIL $size_mib MiB stream retained too much collected live heap" >&2
    exit 1
  fi
  if (( retained_end_live - retained_start_live < retained_growth_min )); then
    echo "FAIL $size_mib MiB readAllBytes control did not retain the payload" >&2
    exit 1
  fi
  if (( retained_rss_kib < retained_rss_min )); then
    echo "FAIL $size_mib MiB readAllBytes control did not raise max RSS" >&2
    exit 1
  fi

  if (( stream_min_rss_kib == 0 || stream_rss_kib < stream_min_rss_kib )); then
    stream_min_rss_kib=$stream_rss_kib
  fi
  if (( stream_rss_kib > stream_max_rss_kib )); then
    stream_max_rss_kib=$stream_rss_kib
  fi
  records+=("$(grep '^QUALIFICATION ' "$stream_output")")
  records+=("$(grep '^QUALIFICATION ' "$retained_output")")
  records+=("CALIBRATION size_mib=$size_mib stream_max_rss_kib=$stream_rss_kib retained_max_rss_kib=$retained_rss_kib stream_end_live_bytes=$stream_end_live stream_end_reserved_bytes=$stream_end_reserved")
done

if (( stream_max_rss_kib - stream_min_rss_kib > rss_plateau_spread_kib )); then
  echo "FAIL streamed max RSS did not plateau across 32/64/128 MiB" >&2
  exit 1
fi

fixture_stats=$(curl -fsS "$endpoint/__fixture_stats__")
fixture_field() {
  local name=$1
  sed -n "s/.*\"$name\": \([0-9][0-9]*\).*/\1/p" <<<"$fixture_stats"
}
fixture_request_chunk=$(fixture_field max_request_chunk_bytes)
fixture_response_chunk=$(fixture_field max_response_chunk_bytes)
fixture_uploaded_bytes=$(fixture_field uploaded_bytes)
fixture_downloaded_bytes=$(fixture_field downloaded_bytes)
if [[ ! $fixture_request_chunk =~ ^[0-9]+$ ||
      ! $fixture_response_chunk =~ ^[0-9]+$ ||
      ! $fixture_uploaded_bytes =~ ^[0-9]+$ ||
      ! $fixture_downloaded_bytes =~ ^[0-9]+$ ]]; then
  echo "FAIL malformed aggregate fixture evidence" >&2
  exit 1
fi
if (( fixture_request_chunk == 0 || fixture_request_chunk > 65536 ||
      fixture_response_chunk == 0 || fixture_response_chunk > 65536 )); then
  echo "FAIL fixture transfer chunks exceeded the 64 KiB streaming bound" >&2
  exit 1
fi
if (( fixture_uploaded_bytes < total_file_bytes ||
      fixture_downloaded_bytes < total_file_bytes * 3 )); then
  echo "FAIL fixture byte totals did not cover one upload and three downloads" >&2
  exit 1
fi
printf '%s\n' "$baseline_record"
printf '%s\n' "${records[@]}"
printf 'RSS_BOUND baseline_max_kib=%s stream_limit_kib=%s plateau_spread_limit_kib=%s live_growth_limit_bytes=%s\n' \
  "$baseline_rss_kib" "$stream_limit_kib" "$rss_plateau_spread_kib" \
  "$live_growth_allowance_bytes"
printf 'FIXTURE_EVIDENCE %s\n' "$fixture_stats"
printf 'ok 32/64/128 MiB checkpoints plateau through production upload and verified recovery; readAllBytes controls scale\n'
