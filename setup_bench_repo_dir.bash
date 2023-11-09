#!/usr/bin/env bash

set -euo pipefail

if [ "$(cat /sys/class/block/nvme1n1/device/model)" != "Amazon EC2 NVMe Instance Storage        " ]; then
    echo "nvme1n1 is not Amazon EC2 NVMe Instance Storage: '$(cat /sys/class/block/nvme1n1/device/model)'"
    exit 1
fi

rmdir bench_repo_dir || true

sudo mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0  /dev/nvme1n1

sudo mount /dev/nvme1n1 /mnt
sudo chown -R "$(id -u)":"$(id -g)" /mnt

mkdir /mnt/bench_repo_dir
mkdir bench_repo_dir
sudo mount --bind /mnt/bench_repo_dir bench_repo_dir

mkdir /mnt/test_output

mkdir /mnt/many_tenants

echo run the following commands

cat <<EOF
    # test suite run
    export TEST_OUTPUT="/mnt/test_output"
    DEFAULT_PG_VERSION=15 BUILD_TYPE=release ./scripts/pytest test_runner/performance/test_pageserver.py

    # for interactive use
    export NEON_REPO_DIR="$(readlink -f ./bench_repo_dir)/repo"
    cargo build_testing --release
    ./target/release/neon_local init
    # ... create tenant, seed it using pgbench
    # then duplicate the tenant using
    # poetry run python3 ./test_runner/duplicate_tenant.py TENANT_ID 200 8
EOF


