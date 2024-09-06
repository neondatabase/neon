#!/bin/bash

# this is a shortcut script to avoid duplication in CI
set -eux -o pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo "Uploading perf report to neon pg"
# ingest per test results data into neon backed postgres running in staging to build grafana reports on that data
DATABASE_URL="$PERF_TEST_RESULT_CONNSTR" poetry run python "$SCRIPT_DIR"/ingest_perf_test_result.py --ingest "$REPORT_FROM"
