
FINAL_SHARDS=4
TENANT_ID=1f359dd625e519a1a4e8d7509690f6fc
ARGS=--release -q

cargo neon $ARGS endpoint stop ep-main

cargo neon $ARGS tenant shard-split --shard-count=$FINAL_SHARDS
cargo neon $ARGS tenant status
cargo neon $ARGS tenant migrate --tenant-id=$TENANT_ID-0004 --id=1
cargo neon $ARGS tenant migrate --tenant-id=$TENANT_ID-0104 --id=2
cargo neon $ARGS tenant migrate --tenant-id=$TENANT_ID-0204 --id=3
cargo neon $ARGS tenant migrate --tenant-id=$TENANT_ID-0304 --id=4
cargo neon $ARGS tenant status

cargo neon $ARGS endpoint start ep-main
