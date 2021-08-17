#!/bin/sh
if [ "$1" = 'pageserver' ]; then
    if [ ! -d "/data/tenants" ]; then
        echo "Initializing pageserver data directory"
        pageserver --init -D /data --postgres-distrib /usr/local
    fi
    echo "Staring pageserver at 0.0.0.0:6400"
    pageserver -l 0.0.0.0:6400 -D /data
else
    "$@"
fi
