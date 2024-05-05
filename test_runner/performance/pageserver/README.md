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

## Set up minio locally

If you want to run test without the cloud setup, you should install [minio](https://min.io/docs/minio/linux/index.html) and
[minio client](https://min.io/docs/minio/linux/reference/minio-mc.html) `mc`.

After installing minio run it using:

```bash
minio server ~/minio --console-address :9001 --address :9000
```
and run the suggested command for setting `myminio` alias.

Before running tests it is important to match the environment variables in your `AWS_PROFILE`
to minio. You can set these variables by creating a new AWS profile with the following
configuration (using aws cli):

```bash
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
REMOTE_STORAGE_S3_REGION=eu-central-1
```

And create a bucket like this:

```bash
mc mb myminio/neon-bucket --region=eu-central-1
```

## Running `test_ondemand_download_churn.py`

To execute the test run the following command with these env variables:

```bash
ENABLE_REAL_S3_REMOTE_STORAGE=true \
REMOTE_STORAGE_S3_BUCKET=neon-bucket \
REMOTE_STORAGE_S3_REGION=eu-central-1 \
AWS_ENDPOINT_URL=http://127.0.0.1:9000 \
AWS_PROFILE=minio \
BUILD_TYPE=release \
DEFAULT_PG_VERSION=15 \
./scripts/pytest test_runner/performance/pageserver/pagebench/test_ondemand_download_churn.py
```
