#!/bin/bash
set -eux -o pipefail
cd "$(dirname "${0}")"
# Takes a variable name as argument. The result is stored in that variable.
generate_id() {
    local -n resvar=$1
    printf -v resvar '%08x%08x%08x%08x' $SRANDOM $SRANDOM $SRANDOM $SRANDOM
}
echo "${OLD_COMPUTE_TAG}"
echo "${NEW_COMPUTE_TAG}"
echo "${TEST_EXTENSIONS_TAG}"
if [ -z "${OLD_COMPUTE_TAG:-}" ] || [ -z "${NEW_COMPUTE_TAG:-}" ] || [ -z "${TEST_EXTENSIONS_TAG:-}" ]; then
  echo OLD_COMPUTE_TAG, NEW_COMPUTE_TAG and TEST_EXTENSIONS_TAG must be set
  exit 1
fi
export PG_VERSION=${PG_VERSION:-16}
export PG_TEST_VERSION=${PG_VERSION}
# Waits for compute node is ready
function wait_for_ready {
  TIME=0
  while ! docker compose logs compute_is_ready | grep -q "accepting connections" && [ ${TIME} -le 300 ] ; do
    ((TIME += 1 ))
    sleep 1
  done
  if [ ${TIME} -gt 300 ]; then
    echo Time is out.
    exit 2
  fi
}
# Creates extensions. Gets a string with space-separated extensions as a parameter
function create_extensions() {
  for ext in ${1}; do
    docker compose exec neon-test-extensions psql -X -v ON_ERROR_STOP=1 -d contrib_regression -c "CREATE EXTENSION IF NOT EXISTS ${ext} CASCADE"
  done
}
# Creates a new timeline. Gets the parent ID and an extension name as parameters.
# Saves the timeline ID in the variable EXT_TIMELINE
function create_timeline() {
  generate_id new_timeline_id

  PARAMS=(
      -sbf
      -X POST
      -H "Content-Type: application/json"
      -d "{\"new_timeline_id\": \"${new_timeline_id}\", \"pg_version\": ${PG_VERSION}, \"ancestor_timeline_id\": \"${1}\"}"
      "http://127.0.0.1:9898/v1/tenant/${tenant_id}/timeline/"
  )
  result=$(curl "${PARAMS[@]}")
  echo $result | jq .
  EXT_TIMELINE[${2}]=${new_timeline_id}
}
# Checks if the timeline ID of the compute node is expected. Gets the timeline ID as a parameter
function check_timeline() {
    TID=$(docker compose exec neon-test-extensions psql -Aqt -c "SHOW neon.timeline_id")
    if [ "${TID}" != "${1}" ]; then
      echo Timeline mismatch
      exit 1
    fi
}
# Restarts the compute node with the required compute tag and timeline.
# Accepts the tag for the compute node and the timeline as parameters.
function restart_compute() {
  docker compose down compute compute_is_ready
  COMPUTE_TAG=${1} TENANT_ID=${tenant_id} TIMELINE_ID=${2} docker compose up --quiet-pull -d --build compute compute_is_ready
  wait_for_ready
  check_timeline ${2}
}
declare -A EXT_TIMELINE
EXTENSIONS='[
{"extname": "plv8", "extdir": "plv8-src"},
{"extname": "vector", "extdir": "pgvector-src"},
{"extname": "unit", "extdir": "postgresql-unit-src"},
{"extname": "hypopg", "extdir": "hypopg-src"},
{"extname": "rum", "extdir": "rum-src"},
{"extname": "ip4r", "extdir": "ip4r-src"},
{"extname": "prefix", "extdir": "prefix-src"},
{"extname": "hll", "extdir": "hll-src"},
{"extname": "pg_cron", "extdir": "pg_cron-src"},
{"extname": "pg_uuidv7", "extdir": "pg_uuidv7-src"},
{"extname": "roaringbitmap", "extdir": "pg_roaringbitmap-src"},
{"extname": "semver", "extdir": "pg_semver-src"},
{"extname": "pg_ivm", "extdir": "pg_ivm-src"},
{"extname": "pgjwt", "extdir": "pgjwt-src"},
{"extname": "pgtap", "extdir": "pgtap-src"},
{"extname": "pg_repack", "extdir": "pg_repack-src"},
{"extname": "h3", "extdir": "h3-pg-src"}
]'
EXTNAMES=$(echo ${EXTENSIONS} | jq -r '.[].extname' | paste -sd ' ' -)
COMPUTE_TAG=${NEW_COMPUTE_TAG} docker compose --profile test-extensions up --quiet-pull --build -d
wait_for_ready
docker compose exec neon-test-extensions psql -c "DROP DATABASE IF EXISTS contrib_regression"
docker compose exec neon-test-extensions psql -c "CREATE DATABASE contrib_regression"
create_extensions "${EXTNAMES}"
query="select json_object_agg(extname,extversion) from pg_extension where extname in ('${EXTNAMES// /\',\'}')"
new_vers=$(docker compose exec neon-test-extensions psql -Aqt -d contrib_regression -c "$query")
docker compose --profile test-extensions down
COMPUTE_TAG=${OLD_COMPUTE_TAG} docker compose --profile test-extensions up --quiet-pull --build -d --force-recreate
wait_for_ready
docker compose exec neon-test-extensions psql -c "DROP DATABASE IF EXISTS contrib_regression"
docker compose exec neon-test-extensions psql -c "CREATE DATABASE contrib_regression"
tenant_id=$(docker compose exec neon-test-extensions psql -Aqt -c "SHOW neon.tenant_id")
EXT_TIMELINE["main"]=$(docker compose exec neon-test-extensions psql -Aqt -c "SHOW neon.timeline_id")
create_timeline "${EXT_TIMELINE["main"]}" init
restart_compute "${OLD_COMPUTE_TAG}" "${EXT_TIMELINE["init"]}"
create_extensions "${EXTNAMES}"
if [ "${FORCE_ALL_UPGRADE_TESTS:-false}" = true ]; then
  exts="${EXTNAMES}"
else
  query="select pge.extname from pg_extension pge join (select key as extname, value as extversion from json_each_text('${new_vers}')) x on pge.extname=x.extname and pge.extversion <> x.extversion"
  exts=$(docker compose exec neon-test-extensions psql -Aqt -d contrib_regression -c "$query")
fi
if [ -z "${exts}" ]; then
  echo "No extensions were upgraded"
else
  for ext in ${exts}; do
    echo Testing ${ext}...
    create_timeline "${EXT_TIMELINE["main"]}" ${ext}
    EXTDIR=$(echo ${EXTENSIONS} | jq -r '.[] | select(.extname=="'${ext}'") | .extdir')
    restart_compute "${OLD_COMPUTE_TAG}" "${EXT_TIMELINE[${ext}]}"
    docker compose exec neon-test-extensions psql -d contrib_regression -c "CREATE EXTENSION ${ext} CASCADE"
    restart_compute "${NEW_COMPUTE_TAG}" "${EXT_TIMELINE[${ext}]}"
    docker compose exec neon-test-extensions psql -d contrib_regression -c "\dx ${ext}"
    if ! docker compose exec neon-test-extensions sh -c /ext-src/${EXTDIR}/test-upgrade.sh; then
      docker  compose exec neon-test-extensions  cat /ext-src/${EXTDIR}/regression.diffs
      exit 1
    fi
    docker compose exec neon-test-extensions psql -d contrib_regression -c "alter extension ${ext} update"
    docker compose exec neon-test-extensions psql -d contrib_regression -c "\dx ${ext}"
  done
fi
