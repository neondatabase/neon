

export RUST_LOG=DEBUG
SHARDS=4
PAGESERVERS=`seq -s , 1 $SHARDS`
SCALE=10
ARGS=--features=testing

set -e

set +e
cargo neon $ARGS stop ; killall -9 storage_broker ; killall -9 safekeeper ; killall -9 pageserver ; killall -9 postgres ; killall -9 attachment_service ; rm -rf .neon
set -e

cargo build --package=pageserver && cargo neon $ARGS init --num-pageservers=$SHARDS && RUST_LOG=debug cargo neon $ARGS start && cargo neon $ARGS tenant create --shard-count=$SHARDS --tenant-id=1f359dd625e519a1a4e8d7509690f6fc --timeline-id=3d34095be52fec4c44a92e774c573b57 --set-default

cargo neon $ARGS endpoint create --pageserver-id=$PAGESERVERS && cargo neon endpoint start --pageserver-id=$PAGESERVERS ep-main

pgbench postgres -i -h 127.0.0.1 -p 55432 -U cloud_admin -s $SCALE

du -sh .neon/local_fs_remote_storage/pageserver/tenants/1f359dd625e519a1a4e8d7509690f6fc*
