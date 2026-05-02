#!/usr/bin/env bash
set -euo pipefail

CONN=${CONN:-"postgresql://appuser:password@endpoint.local.neon.build:4432/postgres?sslmode=verify-full"}
CERT=${CERT:-"/home/charles/db_final_project/neon/server.crt"}
PSQL=${PSQL:-psql}

export PGSSLROOTCERT="$CERT"

check_startup_params() {
  local app_name=$1
  local statement_timeout=$2
  local lock_timeout=$3

  mapfile -t values < <(
    PGAPPNAME="$app_name" \
    PGOPTIONS="-c statement_timeout=$statement_timeout -c lock_timeout=$lock_timeout" \
    "$PSQL" "$CONN" -X -A -t -v ON_ERROR_STOP=1 \
      -c "SHOW application_name; SHOW statement_timeout; SHOW lock_timeout;"
  )

  local got_app_name=${values[0]:-}
  local got_statement_timeout=${values[1]:-}
  local got_lock_timeout=${values[2]:-}

  if [[ "$got_app_name" != "$app_name" ]]; then
    echo "FAIL: application_name expected $app_name, got: $got_app_name" >&2
    exit 1
  fi

  if [[ "$got_statement_timeout" != "$statement_timeout" ]]; then
    echo "FAIL: statement_timeout expected $statement_timeout, got: $got_statement_timeout" >&2
    exit 1
  fi

  if [[ "$got_lock_timeout" != "$lock_timeout" ]]; then
    echo "FAIL: lock_timeout expected $lock_timeout, got: $got_lock_timeout" >&2
    exit 1
  fi

  echo "PASS: startup params applied for $app_name"
}

"$PSQL" "$CONN" -v ON_ERROR_STOP=1 <<'SQL'
SET application_name = 'stale_startup_bad';
SET statement_timeout = '999ms';
SET lock_timeout = '888ms';
SQL

check_startup_params "startup_first" "1111ms" "2111ms"
check_startup_params "startup_second" "2222ms" "3222ms"
