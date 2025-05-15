#!/bin/sh
set -ex
cd "$(dirname ${0})"
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
REGRESS="$(make -n installcheck | awk '{print substr($0,index($0,"init-extension")+15);}')"
${PG_REGRESS} --inputdir=./ --bindir='/usr/local/pgsql/bin'  --use-existing --dbname=contrib_regression ${REGRESS}