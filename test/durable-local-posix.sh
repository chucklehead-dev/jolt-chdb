#!/usr/bin/env bash
set -euo pipefail

wrapper=${1:?usage: durable-local-posix.sh JOLT_WRAPPER}
test_root=$(mktemp -d "${TMPDIR:-/tmp}/jchdb-posix-test.XXXXXX")
cleanup() {
  rm -rf -- "$test_root"
}
trap cleanup EXIT

run_jolt=("$wrapper" jolt -M:durable-local-worker "$test_root")

if [[ $("${run_jolt[@]}" partial-write-probe) != ":ok" ]]; then
  echo "FAIL partial-write retry did not preserve the unwritten suffix" >&2
  exit 1
fi

etag=$("${run_jolt[@]}" create 0)
if [[ ! $etag =~ ^[0-9a-f-]{36}$ ]]; then
  echo "FAIL create did not return an opaque UUID ETag" >&2
  exit 1
fi

case $(uname -s) in
  Linux)
    stat_mode() { stat -c '%a' "$1"; }
    ;;
  Darwin)
    stat_mode() { stat -f '%Lp' "$1"; }
    ;;
  *)
    echo "FAIL unsupported POSIX test host" >&2
    exit 1
    ;;
esac
if [[ $(stat_mode "$test_root/.jchdb.lock") != 600 ||
      $(stat_mode "$test_root/objects") != 700 ||
      $(stat_mode "$test_root/objects/head.json") != 600 ]]; then
  echo "FAIL local backend paths are not private" >&2
  exit 1
fi

new_root="$test_root/new-root"
"$wrapper" jolt -M:durable-local-worker "$new_root" create 9 >/dev/null
if [[ $(stat_mode "$new_root") != 700 ]]; then
  echo "FAIL newly created provider root is not private" >&2
  exit 1
fi

ln -s "$test_root" "$test_root/objects/escape"
escape_type=$("${run_jolt[@]}" read-error-type escape/outside.bin)
if [[ $escape_type != ":jdbc.chdb.durable.backend/unsafe-local-root" ]]; then
  echo "FAIL intermediate symbolic link did not fail closed: $escape_type" >&2
  exit 1
fi
rm "$test_root/objects/escape"

upload_source="$test_root/upload-source.bin"
download_target="$test_root/download-target.bin"
dd if=/dev/zero of="$upload_source" bs=65536 count=3 status=none
printf x >>"$upload_source"
if [[ $("${run_jolt[@]}" put-file "$upload_source") != ":created" ]]; then
  echo "FAIL streaming file upload did not create the object" >&2
  exit 1
fi
download_result=$("${run_jolt[@]}" download "$download_target")
if [[ $download_result != "{:status :downloaded, :byte-count 196609}" ]]; then
  echo "FAIL streaming download result was: $download_result" >&2
  exit 1
fi
if ! cmp -s "$upload_source" "$download_target"; then
  echo "FAIL streamed upload/download bytes differ" >&2
  exit 1
fi
if [[ $(stat_mode "$download_target") != 600 ]]; then
  echo "FAIL streamed download target is not private" >&2
  exit 1
fi

"${run_jolt[@]}" replace "$etag" 1 >"$test_root/a.out" &
writer_a=$!
"${run_jolt[@]}" replace "$etag" 2 >"$test_root/b.out" &
writer_b=$!
wait "$writer_a"
wait "$writer_b"

results=$(sort "$test_root/a.out" "$test_root/b.out" | tr '\n' ' ')
if [[ $results != ":precondition-failed :replaced " ]]; then
  echo "FAIL same-snapshot writers returned: $results" >&2
  exit 1
fi

winner=$("${run_jolt[@]}" read)
if [[ $winner != "1" && $winner != "2" ]]; then
  echo "FAIL committed byte was $winner" >&2
  exit 1
fi

"${run_jolt[@]}" hold-lock 30000 >"$test_root/holder.out" &
holder=$!
ready=false
for _ in $(seq 1 200); do
  if [[ -s $test_root/holder.out ]]; then
    ready=true
    break
  fi
  if ! kill -0 "$holder" 2>/dev/null; then
    break
  fi
  sleep 0.05
done
if [[ $ready != true ]] || ! grep -qx ':locked' "$test_root/holder.out"; then
  echo "FAIL lock holder did not become ready" >&2
  exit 1
fi

kill -9 "$holder"
wait "$holder" 2>/dev/null || true
after_crash=$(timeout 10 "${run_jolt[@]}" read)
if [[ $after_crash != "$winner" ]]; then
  echo "FAIL crash recovery changed the committed byte" >&2
  exit 1
fi

if find "$test_root/objects" -name '.jchdb-*.tmp' -print -quit | grep -q .; then
  echo "FAIL temporary publication file remains" >&2
  exit 1
fi

echo "ok cross-process CAS has exactly one winner"
echo "ok lock, hierarchy, and object modes are private"
echo "ok intermediate symbolic links fail closed"
echo "ok EINTR and repeated partial writes preserve the unwritten suffix"
echo "ok multi-buffer file upload/download is byte-exact and private"
echo "ok process death releases the kernel lock"
echo "ok crash recovery preserves the committed object"
echo "all Durable local POSIX checks passed"
