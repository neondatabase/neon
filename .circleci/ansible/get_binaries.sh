#!/bin/bash

set -e

RELEASE=${RELEASE:-false}

# look at docker hub for latest tag for neon docker image
if [ "${RELEASE}" = "true" ]; then
    echo "search latest relase tag"
    VERSION=$(curl -s https://registry.hub.docker.com/v1/repositories/neondatabase/neon/tags |jq -r -S '.[].name' | grep release | sed 's/release-//g' | tail -1)
    if [ -z "${VERSION}" ]; then
        echo "no any docker tags found, exiting..."
        exit 1
    else
        TAG="release-${VERSION}"
    fi
else
    echo "search latest dev tag"
    VERSION=$(curl -s https://registry.hub.docker.com/v1/repositories/neondatabase/neon/tags |jq -r -S '.[].name' | grep -v release | tail -1)
    if [ -z "${VERSION}" ]; then
        echo "no any docker tags found, exiting..."
        exit 1
    else
        TAG="${VERSION}"
    fi
fi

echo "found ${VERSION}"

# do initial cleanup
rm -rf neon_install postgres_install.tar.gz neon_install.tar.gz .neon_current_version
mkdir neon_install

# retrive binaries from docker image
echo "getting binaries from docker image"
docker pull --quiet neondatabase/neon:${TAG}
ID=$(docker create neondatabase/neon:${TAG})
docker cp ${ID}:/data/postgres_install.tar.gz .
tar -xzf postgres_install.tar.gz -C neon_install
docker cp ${ID}:/usr/local/bin/pageserver neon_install/bin/
docker cp ${ID}:/usr/local/bin/safekeeper neon_install/bin/
docker cp ${ID}:/usr/local/bin/proxy neon_install/bin/
docker cp ${ID}:/usr/local/bin/postgres neon_install/bin/
docker rm -vf ${ID}

# store version to file (for ansible playbooks) and create binaries tarball
echo ${VERSION} > neon_install/.neon_current_version
echo ${VERSION} > .neon_current_version
tar -czf neon_install.tar.gz -C neon_install .

# do final cleaup
rm -rf neon_install postgres_install.tar.gz
