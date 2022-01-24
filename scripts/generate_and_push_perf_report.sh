#!/bin/bash

# this is a shortcut script to avoid duplication in CI

set -eux -o pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

git clone --single-branch --branch testing https://$VIP_VAP_ACCESS_TOKEN@github.com/zenithdb/zenith-perf-data.git  # TODO (PRIOR MERGE) replace back before merge
cd zenith-perf-data
mkdir -p reports/
mkdir -p data/$REPORT_TO

# ingest per test results data into zenith backed postgres running in staging to build grafana reports on that data
DATABASE_URL=$PERF_TEST_RESULT_CONNSTR poetry run python $SCRIPT_DIR/ingest_perf_test_result.py --ingest $REPORT_FROM

cp $REPORT_FROM/* data/$REPORT_TO

echo "Generating report"
poetry run python $SCRIPT_DIR/generate_perf_report_page.py --input-dir data/$REPORT_TO --out reports/$REPORT_TO.html
echo "Uploading perf result"
git add data reports
git \
    -c "user.name=vipvap" \
    -c "user.email=vipvap@zenith.tech" \
    commit \
    --author="vipvap <vipvap@zenith.tech>" \
    -m "add performance test result for $GITHUB_SHA zenith revision"

git push https://$VIP_VAP_ACCESS_TOKEN@github.com/zenithdb/zenith-perf-data.git testing # master TODO (PRIOR MERGE) replace back before merge
