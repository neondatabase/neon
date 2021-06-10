#!/bin/sh
if [ ! -d "/data/timelines" ]; then
    echo "Initializing pageserver data directory"
    pageserver --init --workdir $ZENITH_REPO_DIR
fi
echo "Staring pageserver at 0.0.0.0:6400"
pageserver -l 0.0.0.0:6400 --workdir $ZENITH_REPO_DIR
