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
