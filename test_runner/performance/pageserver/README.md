How to reproduce benchmark results / run these benchmarks interactively.

1. Get an EC2 instance with Instance Store. Use the same instance type as used for the benchmark run.
2. Mount the Instance Store => `neon.git/scripts/ps_ec2_setup_instance_store`
3. Use a pytest command line (see other READMEs further up in the pytest hierarchy).

For tests that take a long time to set up / consume a lot of storage space,
we use the test suite's repo_dir snapshotting functionality (`from_repo_dir`).
It supports mounting snapshots using overlayfs, which improves iteration time.

Here's a full command line.

```
RUST_BACKTRACE=1 NEON_ENV_BUILDER_USE_OVERLAYFS_FOR_SNAPSHOTS=1 DEFAULT_PG_VERSION=15 BUILD_TYPE=release \
    ./scripts/pytest test_runner/performance/pageserver/pagebench/test_pageserver_max_throughput_getpage_at_latest_lsn.py
````

## Running `test_ondemand_download_churn.py`

If you want to run test without the cloud setup, you could use [minio](https://min.io/docs/minio/linux/index.html).
After installing minio run it using:
```bash
minio server ~/minio --console-address :9001 --address :9000
```

After the minio server is running install [minio client](https://min.io/docs/minio/linux/reference/minio-mc.html) `mc`
and create a bucket like this:
```bash
mc mb minio/neon --region=eu-north-1
```

After the local server is set up, we should populate the bucket with data, and the fastest way to do it 
is to just start an instance and run pb_bench.
```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
./target/release/neon_local init
./target/release/neon_local start --pageserver-config-override='remote_storage = { bucket_name = "neon", endpoint = "http://127.0.0.1:9000", bucket_region = "eu-central-1", prefix_in_bucket = "neon", concurrency_limit=500 }'
./target/release/neon_local tenant create --set-default
./target/release/neon_local endpoint create main
./target/release/neon_local endpoint start main
./pg_install/v15/bin/pgbench -i -s 200 'postgresql://cloud_admin@127.0.0.1:55432/postgres'
```

and after this we can just run the test:
```bash
ENABLE_REAL_S3_REMOTE_STORAGE=true \
REMOTE_STORAGE_S3_BUCKET=neon \
REMOTE_STORAGE_S3_REGION=eu-central-1 \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_ENDPOINT_URL=http://127.0.0.1:9000 \
BUILD_TYPE=release \
DEFAULT_PG_VERSION=15 \
./scripts/pytest test_runner/performance/pageserver/pagebench/test_ondemand_download_churn.py
```
