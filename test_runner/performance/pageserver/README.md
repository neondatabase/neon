How to reproduce benchmark results / run these benchmarks interactively.

1. Get an EC2 instance with Instance Store. Use the same instance type as used for the benchmark run.
2. Mount the Instance Store => `neon.git/scripts/ps_ec2_setup_instance_store`
3. Use a pytest command line (see other READMEs further up in the pytest hierarchy).

For tests that take a long time to set up / consume a lot of storage space,
we use the test suite's repo_dir snapshotting functionality (`from_repo_dir`).
It supports mounting snapshots using overlayfs, which improves iteration time.

Here's a full command line.

```
RUST_BACKTRACE=1 NEON_ENV_BUILDER_USE_OVERLAYFS_FOR_SNAPSHOTS=1 DEFAULT_PG_VERSION=16 BUILD_TYPE=release \
    ./scripts/pytest test_runner/performance/pageserver/pagebench/test_pageserver_max_throughput_getpage_at_latest_lsn.py
````
