#!/bin/bash

# A basic test to ensure Docker images are built correctly.
# Build a wrapper around the compute, start all services and runs a simple SQL query.

# to run extension regression tests

# XXX: Current does not work on M1 macs due to x86_64 Docker images compiled only, and no seccomp support in M1 Docker emulation layer.

set -eux -o pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
COMPOSE_FILE=$SCRIPT_DIR/docker-compose.yml

COMPUTE_CONTAINER_NAME=docker-compose-compute-1
SQL="CREATE TABLE t(key int primary key, value text); insert into t values(1,1); select * from t;"
PSQL_OPTION="-h localhost -U postgres -p 55433 -c '$SQL' postgres"
docker exec $COMPUTE_CONTAINER_NAME /bin/bash -c "psql $PSQL_OPTION"


set -eux -o pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
COMPOSE_FILE=extensions_regression_tests/docker-compose.yml

COMPUTE_CONTAINER_NAME=docker-compose-compute-1
SQL="CREATE TABLE t(key int primary key, value text); insert into t values(1,1); select * from t;"
PSQL_OPTION="-h localhost -U cloud_admin -p 55433 -c '$SQL' postgres"

cleanup() {
    echo "show container information"
    docker ps
    docker compose -f $COMPOSE_FILE logs
    echo "stop containers..."
    docker compose -f $COMPOSE_FILE down
}

echo "clean up containers if exists"
cleanup

echo "start containers (pg_version=v15)."
TAG=7910667895 PG_VERSION=v15 docker compose -f $COMPOSE_FILE up --build -d


echo "wait until the compute is ready. timeout after 60s. "
cnt=0
while sleep 1; do
    # check timeout
    cnt=`expr $cnt + 1`
    if [ $cnt -gt 60 ]; then
        echo "timeout before the compute is ready."
        cleanup
        exit 1
    fi

    # check if the compute is ready
    set +o pipefail
    result=`docker compose -f $COMPOSE_FILE logs "compute_is_ready" | grep "accepting connections" | wc -l`
    set -o pipefail
    if [ $result -eq 1 ]; then
        echo "OK. The compute is ready to connect."
        echo "execute simple queries."
        docker exec $COMPUTE_CONTAINER_NAME /bin/bash -c "psql $PSQL_OPTION"
        cleanup
        break
    fi
done
