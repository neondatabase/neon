#!/bin/sh
set -ex
cd "$(dirname ${0})"
dropdb --if-exists contrib_regression
createdb contrib_regression
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --inputdir=./ --bindir='/usr/local/pgsql/bin' --use-existing --inputdir=test --dbname=contrib_regression hypopg hypo_brin hypo_index_part hypo_include hypo_hash hypo_hide_index