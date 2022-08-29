#!/usr/bin/env bash

# This script builds deployable binaries from local code.

set -e
mkdir -p neon_install/bin

# Building the image from scratch can take minutes, since we're recompiling everything,
# including dependencies. To save time we remember the container id so it can be reused
# for incremental compilation.
CONTAINER_ID_PATH="neon_install/container_id"

# Get the CONTAINER_ID, or create a container if one doesn't exist.
if [ -e $CONTAINER_ID_PATH ]
then
    CONTAINER_ID=$(cat $CONTAINER_ID_PATH)
    echo "Using existing container with id: $CONTAINER_ID"
else
    echo "No existing container found. Creating ..."
    docker build \
        --tag "neon:local" \
        --build-arg "REPOSITORY=neondatabase" \
        # Don't use cachepot, it will only slow things down. Instead
        # maybe reuse the same container to get incremental compilation.
        --build-arg "RUSTC_WRAPPER=''" \
        .
	CONTAINER_ID=$(docker create neon:local)
fi

# TODO if files changed locally, cp the code into the image and incrementally re-compile.
# Or maybe mount code as a volume?

# Copy files out of the container
# TODO reuse the second part of .github/ansible/get_binaries.sh to get all the binaries
set -x
docker cp ${CONTAINER_ID}:/usr/local/bin/pageserver neon_install/bin/
