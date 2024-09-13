#!/bin/bash

set -o xtrace  # Print each command before execution

cargo neon stop
rm  -rf .neon

sleep 4

cargo neon init

sleep 3

cargo neon start
sleep 3

export TENANT_ID=14719455a7fbf1d257f427377d096cc2
cargo neon tenant create --pg-version 15 --tenant-id  $TENANT_ID

sleep 1

cargo neon endpoint create main --pg-version 15 --tenant-id $TENANT_ID

sleep 1

cargo neon endpoint start main
cargo neon endpoint list  --tenant-id $TENANT_ID

sleep 3

./pg_install/v15/bin/pgbench -i -s 10 -p 55432 -h 127.0.0.1 -U cloud_admin postgres

# This endpoint runs on version 15
psql -p 55434 -h 127.0.0.1 -U cloud_admin postgres -c "select version();"
psql -p 55432 -h 127.0.0.1 -U cloud_admin postgres -c "select pg_current_wal_lsn()"
psql -p 55432 -h 127.0.0.1 -U cloud_admin postgres -c "\d+"


