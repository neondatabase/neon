#!/bin/sh
set -ex
cd "$(dirname ${0})"
dropdb --if-exist contrib_regression
createdb contrib_regression
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --use-existing --inputdir=./ --bindir='/usr/local/pgsql/bin'    --dbname=contrib_regression pg_cron-test