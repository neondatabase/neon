#!/bin/sh
set -ex
cd "$(dirname ${0})"
make installcheck || true
dropdb --if-exist contrib_regression
createdb contrib_regression
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
sed -i '/hastap/d' test/build/run.sch
sed -Ei 's/\b(aretap|enumtap|ownership|privs|usergroup)\b//g' test/build/run.sch
${PG_REGRESS} --use-existing --dbname=contrib_regression --inputdir=./ --bindir='/usr/local/pgsql/bin'    --inputdir=test --max-connections=879 --schedule test/schedule/main.sch   --schedule test/build/run.sch
