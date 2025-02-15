#!/bin/sh
set -ex
cd "$(dirname ${0})"
patch -p1 <test-upgrade-${PG_VERSION}.patch
psql -d contrib_regression -c "DROP EXTENSION IF EXISTS pgtap"
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --use-existing --inputdir=./ --bindir='/usr/local/pgsql/bin'    --inputdir=test --dbname=contrib_regression base corpus