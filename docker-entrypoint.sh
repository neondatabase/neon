#!/bin/sh
set -eux

if [ "$1" = 'pageserver' ]; then
    if [ ! -d "/data/tenants" ]; then
        echo "Initializing pageserver data directory"
        pageserver --init -D /data -c "pg_distrib_dir='/usr/local'"
    fi
    echo "Staring pageserver at 0.0.0.0:6400"
    pageserver -l 0.0.0.0:6400 --listen-http 0.0.0.0:9898 -D /data
else
    "$@"
fi
