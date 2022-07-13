#!/bin/bash

set -e

RELEASE=${RELEASE:-false}
VERSION="1"
TAG="backport-f88fe021-import-patch-${VERSION}"

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
