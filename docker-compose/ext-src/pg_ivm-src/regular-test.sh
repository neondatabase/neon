#!/bin/sh
set -ex
dropdb --if-exist contrib_regression
createdb contrib_regression
cd "$(dirname ${0})"
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
patch -p1 <regular.patch
${PG_REGRESS} --use-existing --inputdir=./ --bindir='/usr/local/pgsql/bin' --dbname=contrib_regression pg_ivm create_immv refresh_immv
patch -R -p1 <regular.patch