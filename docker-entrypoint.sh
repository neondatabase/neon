#!/bin/sh
set -eux

broker_endpoints_param="${BROKER_ENDPOINT:-absent}"
if [ "$broker_endpoints_param" != "absent" ]; then
    broker_endpoints_param="-c broker_endpoints=['$broker_endpoints_param']"
else
    broker_endpoints_param=''
fi

if [ "$1" = 'pageserver' ]; then
    if [ ! -d "/data/tenants" ]; then
        echo "Initializing pageserver data directory"
        pageserver --init -D /data -c "pg_distrib_dir='/usr/local'" -c "id=10" $broker_endpoints_param
    fi
    echo "Staring pageserver at 0.0.0.0:6400"
    pageserver -c "listen_pg_addr='0.0.0.0:6400'" -c "listen_http_addr='0.0.0.0:9898'" $broker_endpoints_param -D /data
else
    "$@"
fi
