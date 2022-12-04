#!/bin/bash
#
# This script performs the following tasks:
#   - Create a Docker network named "neon"
#   - Start minio server in the "neon" network
#   - Upload neon data to the minio server
#
# Usage: prepare-multi-region.sh {compose|swarm} NEON_DATA
#
#   If the first argument is "compose", this script prepares the environment
#   for a single-node deployment. If it is "swarm", this script prepares the
#   environment for a multi-node deployment.
#
#   NEON_DATA is the path to the neon data directory (typically ".neon") to be
#   uploaded to minio.
#
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 {compose|swarm} NEON_DATA"
  exit 1
fi

mode=""

if [ "$1" = "compose" ]; then
  mode="compose"
elif [ "$1" = "swarm" ]; then
  mode="swarm"
else
  echo "Invalid mode \"$mode\". Must be either \"compose\" or \"swarm\""
  exit 1
fi

set -eu

neon_data=$2

minio_image=quay.io/minio/minio:RELEASE.2022-11-17T23-20-09Z

echo "Starting minio server"

if [ "$mode" = "compose" ]; then
  
  if [ ! -z $(docker container ls -q -f name="minio") ]; then
    echo "Container \"minio\" exists. Please stop and remove it before running this script."
    exit 1
  fi

  if [ -z $(docker network ls -q -f name="neon") ]; then
    echo "Creating bridge network \"neon\""
    docker network create neon
  fi

  docker run --rm -d        \
             --name minio   \
             --network neon \
             -p 9000:9000   \
             -p 9001:9001   \
             $minio_image   \
             server /data --address :9000 --console-address :9001

elif [ "$mode" = "swarm" ]; then

  if [ ! -z $(docker service ls -q -f name="minio") ]; then
    echo "Service \"minio\" exists. Please stop and remove it before running this script."
    exit 1
  fi

  if [ -z $(docker network ls -q -f name="neon") ]; then
    echo "Creating overlay network \"neon\""
    docker network create --driver=overlay neon
  fi

  docker service create --name minio                         \
                        --network neon                       \
                        --publish published=9000,target=9000 \
                        --publish published=9001,target=9001 \
                        $minio_image                         \
                        server /data --address :9000 --console-address :9001

else
  echo "Invalid mode \"$mode\". Something is wrong with the script."
  exit 1
fi

mc_image=quay.io/minio/mc
# Allow alias in shell script
shopt -s expand_aliases 
# Remove the mc-config volume to start fresh
docker volume rm -f mc-config 
alias mc="docker run --rm -it                                         \
                     --network host                                   \
                     --mount source=mc-config,target=/.mc             \
                     --mount type=bind,source=$neon_data,target=/data \
                     $mc_image -C /.mc"

# We use the same safekeeper data directory for all new safekeepers so their IDs
# might not match what is on the safekeeper.id file. Removing the file so that it
# can be regenerated.
rm -f $neon_data/safekeepers/*/safekeeper.id

# Create an alias to the minio server
mc alias set local http://localhost:9000 minioadmin minioadmin

# Create the 'neon' bucket
mc mb local/neon

# Transfer data to minio
dest=local/neon/.neon
echo "Transfering data from \"$neon_data\" to \"$dest\""
mc cp -r /data/tenants $dest
mc cp -r /data/safekeepers $dest
mc cp /data/pageserver.toml $dest

exit 0