#!/bin/sh
set -e

COMPOSE_FILE='docker-compose-extensions.yml'
cd $(dirname $0)
docker-compose -f $COMPOSE_FILE 
COMPUTE_CONTAINER_NAME=docker-compose-compute-1
TEST_CONTAINER_NAME=docker-compose-neon-test-1
PSQL_OPTION="-h localhost -U cloud_admin -p 55433 -d postgres"

cleanup() {
    echo "show container information"
    docker ps
    docker compose -f $COMPOSE_FILE logs
    echo "stop containers..."
    docker compose -f $COMPOSE_FILE down
}

echo "clean up containers if exists"
cleanup

PG_VERSION=16 docker compose -f $COMPOSE_FILE up --build -d

echo "wait until the compute is ready. timeout after 60s. "
cnt=0
while sleep 3; do
    # check timeout
    cnt=`expr $cnt + 3`
    if [ $cnt -gt 60 ]; then
        echo "timeout before the compute is ready."
        cleanup
        exit 1
    fi
    result=`docker compose -f $COMPOSE_FILE logs "compute_is_ready" | grep "accepting connections" | wc -l`
    if [ $result -eq 1 ]; then
        echo "OK. The compute is ready to connect."
        break
    fi
done

echo Enabling trust connection
docker exec $COMPUTE_CONTAINER_NAME bash -c "sed -i '\$d' /var/db/postgres/compute/pg_hba.conf && echo -e 'host\t all\t all\t all\t trust' >> /var/db/postgres/compute/pg_hba.conf"
echo Adding postgres role
docker exec $COMPUTE_CONTAINER_NAME psql $PSQL_OPTION -c "CREATE ROLE postgres SUPERUSER"
echo Adding auto-loading of anon
docker exec $COMPUTE_CONTAINER_NAME psql $PSQL_OPTION -c "ALTER SYSTEM SET session_preload_libraries='anon'"
docker exec $COMPUTE_CONTAINER_NAME psql $PSQL_OPTION -c "SELECT pg_reload_conf()"
TMPDIR=$(mktemp -d)
docker cp $TEST_CONTAINER_NAME:/ext-src/pg_anon-src/data $TMPDIR/data
echo -e '1\t too \t many \t tabs' > $TMPDIR/data/bad.csv
docker cp $TMPDIR/data $COMPUTE_CONTAINER_NAME:/tmp/tmp_anon_alternate_data
rm -rf $TMPDIR
if docker exec -e SKIP=pg_anon-src,rum-src,pg_cron-src,timescaledb-src,rdkit-src,postgis-src,pgx_ulid-src,pgtap-src,pg_tiktoken-src,pg_jsonschema-src,pg_hint_plan-src,pg_graphql-src,kq_imcx-src,wal2json_2_5-src \
    $TEST_CONTAINER_NAME /run-tests.sh > testout.txt
then
    cleanup
    exit 0
else
    FAILED=$(tail -1 testout.txt)
    for d in $FAILED
    do
        mkdir $d
        docker cp $TEST_CONTAINER_NAME:/ext-src/$d/regression.diffs $d || true
        docker cp $TEST_CONTAINER_NAME:/ext-src/$d/regression.out $d || true
    done
    tar czf failed.tar.gz $FAILED
    rm -rf $FAILED
    cleanup
    exit 1
fi