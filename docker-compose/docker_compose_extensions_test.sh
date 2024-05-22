#!/bin/bash

# A test script for postgres extensions
# Currently supports only v16
#
# Implicitly accepts `REPOSITORY` and `TAG` env vars that are passed into the compose file
# Their defaults point at DockerHub `neondatabase/neon:latest` image.`,
# to verify custom image builds (e.g pre-published ones).
set -eux -o pipefail

COMPOSE_FILE='docker-compose-extensions.yml'
cd $(dirname $0)
docker compose -f $COMPOSE_FILE 
COMPUTE_CONTAINER_NAME=docker-compose-compute-1
TEST_CONTAINER_NAME=docker-compose-neon-test-1
PSQL_OPTION="-h localhost -U cloud_admin -p 55433 -d postgres"
: ${http_proxy:=}
: ${https_proxy:=}
export http_proxy https_proxy

cleanup() {
    echo "show container information"
    docker ps
    docker compose -f $COMPOSE_FILE logs
    echo "stop containers..."
    docker compose -f $COMPOSE_FILE down
}

for pg_version in 16; do
    echo "clean up containers if exists"
    cleanup

    PG_VERSION=$pg_version docker compose -f $COMPOSE_FILE up --build -d

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
        if docker compose -f $COMPOSE_FILE logs "compute_is_ready" | grep -q "accepting connections"; then
            echo "OK. The compute is ready to connect."
            break
        fi
    done

    echo Enabling trust connection
    docker exec $COMPUTE_CONTAINER_NAME bash -c "sed -i '\$d' /var/db/postgres/compute/pg_hba.conf && echo -e 'host\t all\t all\t all\t trust' >> /var/db/postgres/compute/pg_hba.conf"
    echo Adding postgres role
    docker exec $COMPUTE_CONTAINER_NAME psql $PSQL_OPTION -c "CREATE ROLE postgres SUPERUSER LOGIN"
    echo Adding auto-loading of anon
    docker exec $COMPUTE_CONTAINER_NAME psql $PSQL_OPTION -c "ALTER SYSTEM SET session_preload_libraries='anon'"
    docker exec $COMPUTE_CONTAINER_NAME psql $PSQL_OPTION -c "SELECT pg_reload_conf()"
    TMPDIR=$(mktemp -d)
    docker cp $TEST_CONTAINER_NAME:/ext-src/pg_anon-src/data $TMPDIR/data
    echo -e '1\t too \t many \t tabs' > $TMPDIR/data/bad.csv
    docker cp $TMPDIR/data $COMPUTE_CONTAINER_NAME:/tmp/tmp_anon_alternate_data
    rm -rf $TMPDIR
    if docker exec -e SKIP=rum-src,pg_cron-src,timescaledb-src,rdkit-src,postgis-src,pgx_ulid-src,pgtap-src,pg_tiktoken-src,pg_jsonschema-src,pg_hint_plan-src,pg_graphql-src,kq_imcx-src,wal2json_2_5-src \
        $TEST_CONTAINER_NAME /run-tests.sh | tee testout.txt
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
            cat $d/regression.out $d/regression.diffs || true
        done
    rm -rf $FAILED
    cleanup
    exit 1
    fi
done