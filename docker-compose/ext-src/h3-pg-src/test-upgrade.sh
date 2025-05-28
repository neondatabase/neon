#!/bin/sh
set -ex
cd "$(dirname ${0})"
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
cd h3/test
TESTS=$(echo sql/* | sed 's|sql/||g; s|\.sql||g')
${PG_REGRESS} --use-existing --inputdir=./ --bindir='/usr/local/pgsql/bin'  --dbname=contrib_regression  ${TESTS}