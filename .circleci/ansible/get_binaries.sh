#!/bin/bash

set -e

RELEASE=${RELEASE:-false}

if [ "${RELEASE}" = "true" ]; then
    echo "search latest relase tag"
    VERSION=$(curl -s https://registry.hub.docker.com/v1/repositories/zenithdb/zenith/tags |jq -r -S '.[].name' | grep release | sed 's/release-//g' | tail -1)
    if [ -z "${VERSION}" ]; then
        echo "no any docker tags found, exiting..."
        exit 1
    else
        TAG="release-${VERSION}"
    fi
else
    echo "search latest dev tag"
    VERSION=$(curl -s https://registry.hub.docker.com/v1/repositories/zenithdb/zenith/tags |jq -r -S '.[].name' | grep -v release | tail -1)
    if [ -z "${VERSION}" ]; then
        echo "no any docker tags found, exiting..."
        exit 1
    else
        TAG="${VERSION}"
    fi
fi

echo "found ${VERSION}"

# do initial cleanup
rm -rf zenith_install postgres_install.tar.gz zenith_install.tar.gz .zenith_current_version
mkdir zenith_install

# retrive binaries from docker image
echo "getting binaries from docker image"
docker pull --quiet zenithdb/zenith:${TAG}
ID=$(docker create zenithdb/zenith:${TAG})
docker cp ${ID}:/data/postgres_install.tar.gz .
tar -xzf postgres_install.tar.gz -C zenith_install
docker cp ${ID}:/usr/local/bin/pageserver zenith_install/bin/
docker cp ${ID}:/usr/local/bin/safekeeper zenith_install/bin/
docker cp ${ID}:/usr/local/bin/proxy zenith_install/bin/
docker cp ${ID}:/usr/local/bin/postgres zenith_install/bin/
docker rm -vf ${ID}

# store version to file (for ansible playbooks) and create binaries tarball
echo ${VERSION} > zenith_install/.zenith_current_version
echo ${VERSION} > .zenith_current_version
tar -czf zenith_install.tar.gz -C zenith_install .

# do final cleaup
rm -rf zenith_install postgres_install.tar.gz
