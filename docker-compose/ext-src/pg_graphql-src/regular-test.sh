#!/bin/bash
set -ex
cd "$(dirname "${0}")"
PGXS="$(dirname "$(pg_config --pgxs)" )"
REGRESS="${PGXS}/../test/regress/pg_regress"
TESTDIR="test"
TESTS=$(ls "${TESTDIR}/sql" | sort )
TESTS=${TESTS//\.sql/}
TESTS=${TESTS/empty_mutations/}
TESTS=${TESTS/function_return_row_is_selectable/}
TESTS=${TESTS/issue_300/}
TESTS=${TESTS/permissions_connection_column/}
TESTS=${TESTS/permissions_functions/}
TESTS=${TESTS/permissions_node_column/}
TESTS=${TESTS/permissions_table_level/}
TESTS=${TESTS/permissions_types/}
TESTS=${TESTS/row_level_security/}
TESTS=${TESTS/sqli_connection/}
dropdb --if-exist contrib_regression
createdb contrib_regression
. ../alter_db.sh
psql -v ON_ERROR_STOP=1 -f test/fixtures.sql -d contrib_regression
${REGRESS} --use-existing --dbname=contrib_regression --inputdir=${TESTDIR} ${TESTS}

