#!/bin/bash

set -e

if [ -n "${DOCKER_TAG}" ]; then
  # Verson is DOCKER_TAG but without prefix
  VERSION=$(echo $DOCKER_TAG | sed 's/^.*-//g')
else
  echo "Please set DOCKER_TAG environment variable"
  exit 1
fi


# do initial cleanup
rm -rf neon_install postgres_install.tar.gz neon_install.tar.gz .neon_current_version
mkdir neon_install

# retrieve binaries from docker image
echo "getting binaries from docker image"
docker pull --quiet neondatabase/neon:${DOCKER_TAG}
ID=$(docker create neondatabase/neon:${DOCKER_TAG})
docker cp ${ID}:/data/postgres_install.tar.gz .
tar -xzf postgres_install.tar.gz -C neon_install
mkdir neon_install/bin/
docker cp ${ID}:/usr/local/bin/pageserver neon_install/bin/
docker cp ${ID}:/usr/local/bin/pageserver_binutils neon_install/bin/
docker cp ${ID}:/usr/local/bin/safekeeper neon_install/bin/
docker cp ${ID}:/usr/local/bin/proxy neon_install/bin/
docker cp ${ID}:/usr/local/v14/bin/ neon_install/v14/bin/
docker cp ${ID}:/usr/local/v15/bin/ neon_install/v15/bin/
docker cp ${ID}:/usr/local/v14/lib/ neon_install/v14/lib/
docker cp ${ID}:/usr/local/v15/lib/ neon_install/v15/lib/
docker rm -vf ${ID}

# store version to file (for ansible playbooks) and create binaries tarball
echo ${VERSION} > neon_install/.neon_current_version
echo ${VERSION} > .neon_current_version
tar -czf neon_install.tar.gz -C neon_install .

# do final cleaup
rm -rf neon_install postgres_install.tar.gz
