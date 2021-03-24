#!/bin/sh
#
# Set up a simple Compute Node + Page Server combination locally.
#
# NOTE: This doesn't clean up between invocations. You'll need to manually:
#
# - Kill any previous 'postgres' and 'pageserver' processes
# - Clear the S3 bucket
# - Remove the 'zenith-pgdata' directory


set -e

# Set up some config.
#
# CHANGE THESE ACCORDING TO YOUR S3 INSTALLATION
export S3_REGION=auto
export S3_ENDPOINT=https://localhost:9000
export S3_ACCESSKEY=minioadmin
export S3_SECRET=pikkunen
export S3_BUCKET=zenith-testbucket


COMPUTE_NODE_PGDATA=zenith-pgdata


# 1. Initialize a cluster.
initdb -D $COMPUTE_NODE_PGDATA -U zenith

echo "port=65432" >> $COMPUTE_NODE_PGDATA/postgresql.conf
echo "log_connections=on" >> $COMPUTE_NODE_PGDATA/postgresql.conf

# Use a small shared_buffers, so that we hit the Page Server more
# easily.
echo "shared_buffers = 1MB" >> $COMPUTE_NODE_PGDATA/postgresql.conf

# TODO: page server should use a replication slot, or some other mechanism
# to make sure that the primary doesn't lose data that the page server still
# needs. (The WAL safekeepers should ensure that)
echo "wal_keep_size=10GB" >> $COMPUTE_NODE_PGDATA/postgresql.conf

# Tell the Postgres server how to connect to the Page Server
echo "page_server_connstring='host=localhost port=5430'" >> $COMPUTE_NODE_PGDATA/postgresql.conf


# 2. Run zenith_push to push a base backup fo the database to an S3 bucket. The
# Page Server will read it from there
zenith_push -D $COMPUTE_NODE_PGDATA


# 3. Launch page server
rm -rf /tmp/pgdata-dummy
initdb -N -D /tmp/pgdata-dummy
PGDATA=/tmp/pgdata-dummy ./target/debug/pageserver  &

# 4. Start up the Postgres server
postgres -D $COMPUTE_NODE_PGDATA &


echo "ALL SET! You can now connect to Postgres with something like:"
echo ""
echo 'psql "dbname=postgres host=localhost user=zenith port=65432"'
