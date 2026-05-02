#!/usr/bin/env bash
set -euo pipefail

CONN=${CONN:-"postgresql://appuser:password@endpoint.local.neon.build:4432/postgres?sslmode=verify-full"}
CERT=${CERT:-"/home/charles/db_final_project/neon/server.crt"}
PSQL=${PSQL:-psql}

export PGSSLROOTCERT="$CERT"

"$PSQL" "$CONN" -v ON_ERROR_STOP=1 <<'SQL'
SET application_name = 'leak_test_bad';
SET statement_timeout = '1234ms';
BEGIN;
SET LOCAL lock_timeout = '999ms';
ROLLBACK;
SQL

mapfile -t values < <(
  "$PSQL" "$CONN" -X -A -t -v ON_ERROR_STOP=1 \
    -c "SHOW application_name; SHOW statement_timeout; SHOW lock_timeout;"
)

application_name=${values[0]:-}
statement_timeout=${values[1]:-}
lock_timeout=${values[2]:-}

if [[ "$application_name" == "leak_test_bad" ]]; then
  echo "FAIL: application_name leaked: $application_name" >&2
  exit 1
fi

if [[ "$statement_timeout" != "0" ]]; then
  echo "FAIL: statement_timeout leaked: $statement_timeout" >&2
  exit 1
fi

if [[ "$lock_timeout" != "0" ]]; then
  echo "FAIL: lock_timeout expected 0, got: $lock_timeout" >&2
  exit 1
fi

echo "PASS: no session state leaked"
echo "application_name=$application_name"
echo "statement_timeout=$statement_timeout"
echo "lock_timeout=$lock_timeout"
