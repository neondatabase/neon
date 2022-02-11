#!/bin/bash

# this is a shortcut script to avoid duplication in CI
set -eux -o pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo "Uploading perf report to zenith pg"
# ingest per test results data into zenith backed postgres running in staging to build grafana reports on that data
DATABASE_URL="$PERF_TEST_RESULT_CONNSTR" poetry run python "$SCRIPT_DIR"/ingest_perf_test_result.py --ingest "$REPORT_FROM"

# Activate poetry's venv. Needed because git upload does not run in a project dir (it uses tmp to store the repository)
# so the problem occurs because poetry cannot find pyproject.toml in temp dir created by git upload
# shellcheck source=/dev/null
. "$(poetry env info --path)"/bin/activate

echo "Uploading perf result to zenith-perf-data"
scripts/git-upload \
    --repo=https://"$VIP_VAP_ACCESS_TOKEN"@github.com/zenithdb/zenith-perf-data.git \
    --message="add performance test result for $GITHUB_SHA zenith revision" \
    --branch=master \
    copy "$REPORT_FROM" "data/$REPORT_TO" `# COPY FROM TO_RELATIVE`\
    --merge \
    --run-cmd "python $SCRIPT_DIR/generate_perf_report_page.py --input-dir data/$REPORT_TO --out reports/$REPORT_TO.html"
