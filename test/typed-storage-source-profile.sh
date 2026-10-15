#!/usr/bin/env bash
set -euo pipefail
test "$#" -eq 2
baseline=$1
candidate=$2
check="$candidate/bench/check-typed-storage-source.sh"
reject() {
  if "$@"; then
    echo unexpected-source-profile-acceptance >&2
    exit 1
  fi
}
env -u QUALIFIED_STORAGE_SOURCE_PROFILE -u SDK_S3_EXPECT_WAIT_MS bash "$check" "$baseline"
reject env -u QUALIFIED_STORAGE_SOURCE_PROFILE -u SDK_S3_EXPECT_WAIT_MS bash "$check" "$candidate"
for wait in 1000 10; do
  env QUALIFIED_STORAGE_SOURCE_PROFILE=expect-continue-ab SDK_STORAGE_ARM=s3 SDK_S3_EXPECT_WAIT_MS="$wait" bash "$check" "$candidate"
done
reject env QUALIFIED_STORAGE_SOURCE_PROFILE=expect-continue-ab SDK_STORAGE_ARM=s3 SDK_S3_EXPECT_WAIT_MS=1 bash "$check" "$candidate"
reject env QUALIFIED_STORAGE_SOURCE_PROFILE=expect-continue-ab SDK_STORAGE_ARM=local SDK_S3_EXPECT_WAIT_MS=10 bash "$check" "$candidate"
reject env QUALIFIED_STORAGE_SOURCE_PROFILE=expect-continue-ab SDK_STORAGE_ARM=s3 SDK_S3_EXPECT_WAIT_MS=10 bash "$check" "$baseline"
reject env QUALIFIED_STORAGE_SOURCE_PROFILE=unreviewed SDK_STORAGE_ARM=s3 SDK_S3_EXPECT_WAIT_MS=10 bash "$check" "$candidate"
reject env QUALIFIED_STORAGE_SOURCE_PROFILE=baseline SDK_S3_EXPECT_WAIT_MS=10 bash "$check" "$baseline"
echo '9 source profile controls passed'
