#!/usr/bin/env bash
set -ex
cd "$(dirname "${0}")"
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
dropdb --if-exists contrib_regression
createdb contrib_regression
cd h3_postgis/test
psql -d contrib_regression -c "CREATE EXTENSION postgis" -c "CREATE EXTENSION postgis_raster" -c "CREATE EXTENSION h3" -c "CREATE EXTENSION h3_postgis"
TESTS=$(echo sql/* | sed 's|sql/||g; s|\.sql||g')
${PG_REGRESS} --use-existing --dbname contrib_regression ${TESTS}
cd ../../h3/test
TESTS=$(echo sql/* | sed 's|sql/||g; s|\.sql||g')
dropdb --if-exists contrib_regression
createdb contrib_regression
psql -d contrib_regression -c "CREATE EXTENSION h3"
${PG_REGRESS} --use-existing --dbname contrib_regression ${TESTS}
