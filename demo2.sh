#!/bin/bash

set -o xtrace  # Print each command before execution

# stop endpoint. Right now this is important, because pg_upgrade will start it
# This is not strictly needed, so with some hacking we can implement upgrade without a pause.

cargo neon endpoint stop main
cargo neon endpoint list  --tenant-id $TENANT_ID
 
# Let's create branch with new major postgres version
# !This is the feature that we developed during the hackathon!
# everything else is setup and checks

cargo neon timeline branch --tenant-id $TENANT_ID --pg-version 16 --branch-name branch_16

# create and start endpoint on it
cargo neon endpoint create ep_16 --pg-version 16 --tenant-id $TENANT_ID  --branch-name branch_16

cargo neon endpoint start ep_16

# let's ensure that this new endpoint runs on a new version
psql -p 55434 -h 127.0.0.1 -U cloud_admin postgres -c "select version();"

psql -p 55434 -h 127.0.0.1 -U cloud_admin postgres -c "select pg_current_wal_lsn()"


# This will show 0 bytes size for all user relations
# This is a known issue. 
# New timeline doesn't have these extensions, we will read them from parent.
# Now relsize cache for them is also empty. After SeqScan this size cache fill be correct.
# We need to copy the relsize cache from parent timeline.

psql -p 55434 -h 127.0.0.1 -U cloud_admin postgres -c "\d+"

# And as you can see, there is some data in the new endpoint.
psql -p 55434 -h 127.0.0.1 -U cloud_admin postgres -c "select count(*) from pgbench_accounts;"
psql -p 55434 -h 127.0.0.1 -U cloud_admin postgres -c "select count(*) from pgbench_branches;"
psql -p 55434 -h 127.0.0.1 -U cloud_admin postgres -c "select count(*) from pgbench_tellers;"

psql -p 55434 -h 127.0.0.1 -U cloud_admin postgres -c "\d+"
