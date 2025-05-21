#!/bin/sh
set -ex
cd "$(dirname "${0}")"
dropdb --if-exist contrib_regression
createdb contrib_regression
. ../alter_db.sh
psql -d contrib_regression -c "CREATE EXTENSION vector" -c "CREATE EXTENSION rag"
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --inputdir=./ --bindir='/usr/local/pgsql/bin'    --use-existing --load-extension=vector --load-extension=rag --dbname=contrib_regression basic_functions text_processing api_keys chunking_functions document_processing embedding_api_functions voyageai_functions
