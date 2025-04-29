#!/bin/sh
set -ex
cd "$(dirname ${0})"
dropdb --if-exist contrib_regression
createdb contrib_regression
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --use-existing  --inputdir=./ --bindir='/usr/local/pgsql/bin'    --encoding=UTF8 --dbname=contrib_regression init base_plan pg_hint_plan ut-init ut-A ut-S ut-J ut-L ut-G ut-R ut-fdw ut-W ut-T ut-fini hints_anywhere plpgsql oldextversions