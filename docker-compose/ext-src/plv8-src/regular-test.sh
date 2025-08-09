#!/bin/bash
set -ex
cd "$(dirname ${0})"
dropdb --if-exist contrib_regression
createdb contrib_regression
. ../alter_db.sh
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
REGRESS="$(make -n installcheck | awk '{print substr($0,index($0,"init-extension"));}')"
REGRESS="${REGRESS/startup_perms/}"
REGRESS="${REGRESS/startup /}"
REGRESS="${REGRESS/find_function_perms/}"
REGRESS="${REGRESS/guc/}"
${PG_REGRESS} --inputdir=./ --bindir='/usr/local/pgsql/bin'  --use-existing --dbname=contrib_regression ${REGRESS}