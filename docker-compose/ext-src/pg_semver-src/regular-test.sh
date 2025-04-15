#!/bin/bash
set -ex
# For v16 it's required to create a type which is impossible without superuser access
# do not run this test so far
if [[ "${PG_VERSION}" = v16 ]]; then
  exit 0
fi
cd "$(dirname ${0})"
dropdb --if-exist contrib_regression
createdb contrib_regression
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --use-existing --inputdir=./ --bindir='/usr/local/pgsql/bin'    --inputdir=test --dbname=contrib_regression base corpus