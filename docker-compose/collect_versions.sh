#!/bin/bash
set -eux -o pipefail
export TAG=6659
export PGUSER=cloud_admin
export PGPASSWORD=cloud_admin
export PGHOST=127.0.0.1
export PGPORT=55433
export PGDATABASE=postgres
docker volume prune -f
function wait_for_ready {
  while ! docker compose logs compute_is_ready | grep -q "accepting connections"; do
    sleep 1
  done
}
EXTENSIONS='[
{"extname": "plv8", "extdir": "plv8-src"},
{"extname": "vector", "extdir": "pgvector-src"}
]'
EXTNAMES=$(echo ${EXTENSIONS} | jq -r '.[].extname' | paste -sd ' ' -)
TAG=6660 docker compose up --build -d
wait_for_ready
for ext in $EXTNAMES; do
  echo "CREATE EXTENSION IF NOT EXISTS ${ext};"
done | psql -X -v ON_ERROR_STOP=1
query="select json_object_agg(extname,extversion) from pg_extension where extname in ('${EXTNAMES// /','}')"
new_vers=$(psql -Aqt -c "$query" )
echo $new_vers
docker compose down
TAG=6659 docker compose --profile test-extensions up --build -d --force-recreate
wait_for_ready
for ext in $EXTNAMES; do
  echo "CREATE EXTENSION IF NOT EXISTS ${ext};"
done | psql -X -v ON_ERROR_STOP=1
query="select pge.extname from pg_extension pge join (select key as extname, value as extversion from json_each_text('${new_vers}')) x on pge.extname=x.extname and pge.extversion <> x.extversion"
#query="select * from pg_extension"
echo $query
exts=$(psql -Aqt -c "$query")
if [ -z "${exts}" ]; then
  echo "No extensions were upgraded"
else
  for ext in ${exts}; do
    echo Testing ${ext}...
    EXTDIR=$(echo ${EXTENSIONS} | jq -r '.[] | select(.extname=="'${ext}'") | .extdir')
    docker compose exec -e PGPASSWORD=cloud_admin neon-test-extensions bash -c "cd /ext-src/${EXTDIR} && make installcheck"
    TAG=6659 docker compose down compute
    COMPUTE_TAG=6660 TAG=6659 docker compose up -d --build compute
    wait_for_ready
    docker compose exec -e PGPASSWORD=cloud_admin neon-test-extensions bash -c "cd /ext-src/${EXTDIR} && /usr/local/pgsql/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --bindir='/usr/local/pgsql/bin' --inputdir=test --use-existing --dbname=contrib_regression bit btree cast copy halfvec hnsw_bit hnsw_halfvec hnsw_sparsevec hnsw_vector ivfflat_bit ivfflat_halfvec ivfflat_vector sparsevec vector_type"
    psql -d contrib_regression -c '\dx'
    psql -d contrib_regression -c "alter extension ${ext} update"
    psql -d contrib_regression -c '\dx'
    docker compose exec -e PGPASSWORD=cloud_admin neon-test-extensions bash -c "cd /ext-src/${EXTDIR} && /usr/local/pgsql/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --bindir='/usr/local/pgsql/bin' --inputdir=test --use-existing --dbname=contrib_regression bit btree cast copy halfvec hnsw_bit hnsw_halfvec hnsw_sparsevec hnsw_vector ivfflat_bit ivfflat_halfvec ivfflat_vector sparsevec vector_type"
  done
fi
docker compose --profile test-extensions down