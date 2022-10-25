#!/bin/bash

# this is a shortcut script to avoid duplication in CI
set -eux -o pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
COMPOSE_FILE=$SCRIPT_DIR/../docker-compose/docker-compose.yml

cleanup() {
	docker ps
	docker-compose -f $COMPOSE_FILE logs
	echo "stop containers..."
	docker-compose -f $COMPOSE_FILE down
}

echo "clean up containers if exists"
cleanup

for compute_version in v14 v15; do
	echo "start containers (compute_version=$compute_version)."
	COMPUTE_VERSION=$compute_version TAG=latest docker-compose -f $COMPOSE_FILE up --build -d 

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
		result=`docker-compose -f $COMPOSE_FILE logs "compute_is_ready" | grep "accepting connections" | wc -l`
		set -o pipefail
		if [ $result -eq 1 ]; then
			echo "OK. The compute is ready to connect."
			cleanup
			break
		fi
	done
done