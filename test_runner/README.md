## Neon test runner

This directory contains integration tests.

Prerequisites:
- Correctly configured Python, see [`/docs/sourcetree.md`](/docs/sourcetree.md#using-python)
- Neon and Postgres binaries
    - See the root [README.md](/README.md) for build directions
      To run tests you need to add `--features testing` to Rust code build commands.
      For convenience, repository cargo config contains `build_testing` alias, that serves as a subcommand, adding the required feature flags.
      Usage example: `cargo build_testing --release` is equivalent to `cargo build --features testing --release`
    - Tests can be run from the git tree; or see the environment variables
      below to run from other directories.
- The neon git repo, including the postgres submodule
  (for some tests, e.g. `pg_regress`)

### Test Organization

Regression tests are in the 'regress' directory. They can be run in
parallel to minimize total runtime. Most regression test sets up their
environment with its own pageservers and safekeepers.

'pg_clients' contains tests for connecting with various client
libraries. Each client test uses a Dockerfile that pulls an image that
contains the client, and connects to PostgreSQL with it. The client
tests can be run against an existing PostgreSQL or Neon installation.

'performance' contains performance regression tests. Each test
exercises a particular scenario or workload, and outputs
measurements. They should be run serially, to avoid the tests
interfering with the performance of each other. Some performance tests
set up their own Neon environment, while others can be run against an
existing PostgreSQL or Neon environment.

### Running the tests

There is a wrapper script to invoke pytest: `./scripts/pytest`.
It accepts all the arguments that are accepted by pytest.
Depending on your installation options pytest might be invoked directly.

Test state (postgres data, pageserver state, and log files) will
be stored under a directory `test_output`.

You can run all the tests with:

`./scripts/pytest`

If you want to run all the tests in a particular file:

`./scripts/pytest test_pgbench.py`

If you want to run all tests that have the string "bench" in their names:

`./scripts/pytest -k bench`

To run tests in parellel we utilize `pytest-xdist` plugin. By default everything runs single threaded. Number of workers can be specified with `-n` argument:

`./scripts/pytest -n4`

By default performance tests are excluded. To run them explicitly pass performance tests selection to the script:

`./scripts/pytest test_runner/performance`

Useful environment variables:

`NEON_BIN`: The directory where neon binaries can be found.
`COMPATIBILITY_NEON_BIN`: The directory where the previous version of Neon binaries can be found
`POSTGRES_DISTRIB_DIR`: The directory where postgres distribution can be found.
Since pageserver supports several postgres versions, `POSTGRES_DISTRIB_DIR` must contain
a subdirectory for each version with naming convention `v{PG_VERSION}/`.
Inside that dir, a `bin/postgres` binary should be present.
`COMPATIBILITY_POSTGRES_DISTRIB_DIR`: The directory where the prevoius version of postgres distribution can be found.
`DEFAULT_PG_VERSION`: The version of Postgres to use,
This is used to construct full path to the postgres binaries.
Format is 2-digit major version nubmer, i.e. `DEFAULT_PG_VERSION=16`
`TEST_OUTPUT`: Set the directory where test state and test output files
should go.
`RUST_LOG`: logging configuration to pass into Neon CLI

Useful parameters and commands:

`--preserve-database-files` to preserve pageserver (layer) and safekeer (segment) timeline files on disk
after running a test suite. Such files might be large, so removed by default; but might be useful for debugging or creation of svg images with layer file contents. If `NeonEnvBuilder#preserve_database_files` set to `True` for a particular test, the whole `repo` directory will be attached to Allure report (thus uploaded to S3) as `everything.tar.zst` for this test.

Let stdout, stderr and `INFO` log messages go to the terminal instead of capturing them:
`./scripts/pytest -s --log-cli-level=INFO ...`
(Note many tests capture subprocess outputs separately, so this may not
show much.)

Exit after the first test failure:
`./scripts/pytest -x ...`
(there are many more pytest options; run `pytest -h` to see them.)

#### Running Python tests against real S3 or S3-compatible services

Neon's `libs/remote_storage` supports multiple implementations of remote storage.
At the time of writing, that is
```rust
pub enum RemoteStorageKind {
    /// Storage based on local file system.
    /// Specify a root folder to place all stored files into.
    LocalFs(Utf8PathBuf),
    /// AWS S3 based storage, storing all files in the S3 bucket
    /// specified by the config
    AwsS3(S3Config),
    /// Azure Blob based storage, storing all files in the container
    /// specified by the config
    AzureContainer(AzureConfig),
}
```

The test suite has a Python enum with equal name but different meaning:

```python
@enum.unique
class RemoteStorageKind(StrEnum):
    LOCAL_FS = "local_fs"
    MOCK_S3 = "mock_s3"
    REAL_S3 = "real_s3"
```

* `LOCAL_FS` => `LocalFs`
* `MOCK_S3`: starts [`moto`](https://github.com/getmoto/moto)'s S3 implementation, then configures Pageserver with `AwsS3`
* `REAL_S3` => configure `AwsS3` as detailed below

When a test in the test suite needs an `AwsS3`, it is supposed to call `remote_storage.s3_storage()`.
That function checks env var `ENABLE_REAL_S3_REMOTE_STORAGE`:
* If it is not set, use `MOCK_S3`
* If it is set, use `REAL_S3`.

For `REAL_S3`, the test suite creates the dict/toml representation of the `RemoteStorageKind::AwsS3` based on env vars:

```rust
pub struct S3Config {
    // test suite env var: REMOTE_STORAGE_S3_BUCKET
    pub bucket_name: String,
    // test suite env var: REMOTE_STORAGE_S3_REGION
    pub bucket_region: String,
    // test suite determines this
    pub prefix_in_bucket: Option<String>,
    // no env var exists; test suite sets it for MOCK_S3, because that's how moto works
    pub endpoint: Option<String>,
    ...
}
```

*Credentials* are not part of the config, but discovered by the AWS SDK.
See the `libs/remote_storage` Rust code.
We're documenting two mechanism here:

The test suite supports two mechanisms (`remote_storage.py`):

**Credential mechanism 1**: env vars `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
Populate the env vars with AWS access keys that you created in IAM.
Our CI uses this mechanism.
However, it is _not_ recommended for interactive use by developers ([learn more](https://docs.aws.amazon.com/sdkref/latest/guide/access-users.html#credentials-long-term)).
Instead, use profiles (next section).

**Credential mechanism 2**: env var `AWS_PROFILE`.
This uses the AWS SDK's (and CLI's) profile mechanism.
Learn more about it [in the official docs](https://docs.aws.amazon.com/sdkref/latest/guide/file-format.html).
After configuring a profile (e.g. via the aws CLI), set the env var to its name.

In conclusion, the full command line is:

```bash
# with long-term AWS access keys
ENABLE_REAL_S3_REMOTE_STORAGE=true \
REMOTE_STORAGE_S3_BUCKET=mybucket \
REMOTE_STORAGE_S3_REGION=eu-central-1 \
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
./scripts/pytest
```
<!-- Don't forget to update the Minio example when changing these -->
```bash
# with AWS PROFILE
ENABLE_REAL_S3_REMOTE_STORAGE=true \
REMOTE_STORAGE_S3_BUCKET=mybucket \
REMOTE_STORAGE_S3_REGION=eu-central-1 \
AWS_PROFILE=... \
./scripts/pytest
```

If you're using SSO, make sure to `aws sso login --profile $AWS_PROFILE` first.

##### Minio

If you want to run test without the cloud setup, we recommend [minio](https://min.io/docs/minio/linux/index.html).

```bash
# Start in Terminal 1
mkdir /tmp/minio_data
minio server /tmp/minio_data --console-address 127.0.0.1:9001 --address 127.0.0.1:9000
```

In another terminal, create an `aws` CLI profile for it:

```ini
# append to ~/.aws/config
[profile local-minio]
services = local-minio-services
[services local-minio-services]
s3 =
  endpoint_url=http://127.0.0.1:9000/
```


Now configure the credentials (this is going to write `~/.aws/credentials` for you).
It's an interactive prompt.

```bash
# Terminal 2
$ aws --profile local-minio configure
AWS Access Key ID [None]: minioadmin
AWS Secret Access Key [None]: minioadmin
Default region name [None]:
Default output format [None]:
```

Now create a bucket `testbucket` using the CLI.

```bash
# (don't forget to have AWS_PROFILE env var set; or use --profile)
aws --profile local-minio s3 mb s3://mybucket
```

(If it doesn't work, make sure you update your AWS CLI to a recent version.
 The [service-specific endpoint feature](https://docs.aws.amazon.com/sdkref/latest/guide/feature-ss-endpoints.html)
 that we're using is quite new.)

```bash
# with AWS PROFILE
ENABLE_REAL_S3_REMOTE_STORAGE=true \
REMOTE_STORAGE_S3_BUCKET=mybucket \
REMOTE_STORAGE_S3_REGION=doesntmatterforminio \
AWS_PROFILE=local-minio \
./scripts/pytest
```

NB: you can avoid the `--profile` by setting the `AWS_PROFILE` variable.
Just like the AWS SDKs, the `aws` CLI is sensible to it.

#### Running Rust tests against real S3 or S3-compatible services

We have some Rust tests that only run against real S3, e.g., [here](https://github.com/neondatabase/neon/blob/c18d3340b5e3c978a81c3db8b6f1e83cd9087e8a/libs/remote_storage/tests/test_real_s3.rs#L392-L397).

They use the same env vars as the Python test suite (see previous section)
but interpret them on their own.
However, at this time, the interpretation is identical.

So, above instructions apply to the Rust test as well.

### Writing a test

Every test needs a Neon Environment, or NeonEnv to operate in. A Neon Environment
is like a little cloud-in-a-box, and consists of a Pageserver, 0-N Safekeepers, and
compute Postgres nodes. The connections between them can be configured to use JWT
authentication tokens, and some other configuration options can be tweaked too.

The easiest way to get access to a Neon Environment is by using the `neon_simple_env`
fixture. For convenience, there is a branch called `main` in environments created with
'neon_simple_env', ready to be used in the test.

For more complicated cases, you can build a custom Neon Environment, with the `neon_env`
fixture:

```python
def test_foobar(neon_env_builder: NeonEnvBuilder):
    # Prescribe the environment.
    # We want to have 3 safekeeper nodes, and use JWT authentication in the
    # connections to the page server
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.set_pageserver_auth(True)

    # Now create the environment. This initializes the repository, and starts
    # up the page server and the safekeepers
    env = neon_env_builder.init_start()

    # Run the test
    ...
```

The env includes a default tenant and timeline. Therefore, you do not need to create your own
tenant/timeline for testing.

```python
def test_foobar2(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start() # Start the environment
    with env.endpoints.create_start("main") as endpoint:
        # Start the compute endpoint
    client = env.pageserver.http_client() # Get the pageserver client

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    client.timeline_detail(tenant_id=tenant_id, timeline_id=timeline_id)
```

All the test which rely on NeonEnvBuilder, can check the various version combinations of the components.
To do this yuo may want to add the parametrize decorator with the function fixtures.utils.allpairs_versions()
E.g.

```python
@pytest.mark.parametrize(**fixtures.utils.allpairs_versions())
def test_something(
...
```

For more information about pytest fixtures, see https://docs.pytest.org/en/stable/fixture.html

At the end of a test, all the nodes in the environment are automatically stopped, so you
don't need to worry about cleaning up. Logs and test data are preserved for the analysis,
in a directory under `../test_output/<testname>`

### Before submitting a patch
Ensure that you pass all [obligatory checks](/docs/sourcetree.md#obligatory-checks).

Also consider:

* Writing a couple of docstrings to clarify the reasoning behind a new test.
* Adding more type hints to your code to avoid `Any`, especially:
  * For fixture parameters, they are not automatically deduced.
  * For function arguments and return values.
