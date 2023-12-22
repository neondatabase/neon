

export RUST_LOG=DEBUG
INITIAL_SHARDS=1
PAGESERVERS=8
FINAL_SHARDS=8
STRIPE_SIZE=1024
SCALE=10
ARGS=--release
RUST_LOG=info

TENANT_ID=1f359dd625e519a1a4e8d7509690f6fc

set -e
set -x

set +e
cargo neon $ARGS stop ; killall -9 storage_broker ; killall -9 safekeeper ; killall -9 pageserver ; killall -9 postgres ; killall -9 attachment_service ; rm -rf .neon
set -e

cargo build $ARGS --features=testing

cargo neon $ARGS init --num-pageservers=$PAGESERVERS && RUST_LOG=$RUST_LOG cargo neon $ARGS start && cargo neon $ARGS tenant create --shard-count=$INITIAL_SHARDS --shard-stripe-size=$STRIPE_SIZE  --tenant-id=$TENANT_ID --timeline-id=3d34095be52fec4c44a92e774c573b57 --set-default

cargo neon $ARGS endpoint create && cargo neon $ARGS endpoint start ep-main

pgbench postgres -i -h 127.0.0.1 -p 55432 -U cloud_admin -s $SCALE

cargo neon $ARGS tenant status

# pgbench postgres -h 127.0.0.1 -p 55432 -U cloud_admin -T 600 -P 1 -c 32
