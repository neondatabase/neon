#!/bin/sh
set -eux

if [ "$1" = 'pageserver' ]; then
    if [ ! -d "/data/tenants" ]; then
        echo "Initializing pageserver data directory"
        pageserver --init -D /data -c "pg_distrib_dir='/usr/local'" -c "id=10"
    fi
    echo "Staring pageserver at 0.0.0.0:6400"
    pageserver -c "listen_pg_addr='0.0.0.0:6400'" -c "listen_http_addr='0.0.0.0:9898'" -D /data
else
    "$@"
fi
