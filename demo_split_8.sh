
FINAL_SHARDS=8
TENANT_ID=1f359dd625e519a1a4e8d7509690f6fc
ARGS="--release -q"

cargo neon $ARGS endpoint stop ep-main

cargo neon $ARGS tenant shard-split --shard-count=$FINAL_SHARDS
cargo neon $ARGS tenant status
cargo neon $ARGS tenant migrate --tenant-id=$TENANT_ID-0008 --id=1
cargo neon $ARGS tenant migrate --tenant-id=$TENANT_ID-0108 --id=2
cargo neon $ARGS tenant migrate --tenant-id=$TENANT_ID-0208 --id=3
cargo neon $ARGS tenant migrate --tenant-id=$TENANT_ID-0308 --id=4
cargo neon $ARGS tenant migrate --tenant-id=$TENANT_ID-0408 --id=5
cargo neon $ARGS tenant migrate --tenant-id=$TENANT_ID-0508 --id=6
cargo neon $ARGS tenant migrate --tenant-id=$TENANT_ID-0608 --id=7
cargo neon $ARGS tenant migrate --tenant-id=$TENANT_ID-0708 --id=8
cargo neon $ARGS tenant status

cargo neon $ARGS endpoint start ep-main
