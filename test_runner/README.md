## Zenith test runner

This directory contains integration tests.

Prerequisites:
- Correctly configured Python, see [`/docs/sourcetree.md`](/docs/sourcetree.md#using-python)
- Zenith and Postgres binaries
    - See the root [README.md](/README.md) for build directions
    - Tests can be run from the git tree; or see the environment variables
      below to run from other directories.
- The zenith git repo, including the postgres submodule
  (for some tests, e.g. `pg_regress`)

### Test Organization

The tests are divided into a few batches, such that each batch takes roughly
the same amount of time. The batches can be run in parallel, to minimize total
runtime. Currently, there are only two batches:

- test_batch_pg_regress: Runs PostgreSQL regression tests
- test_others: All other tests

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

Useful environment variables:

`ZENITH_BIN`: The directory where zenith binaries can be found.
`POSTGRES_DISTRIB_DIR`: The directory where postgres distribution can be found.
`TEST_OUTPUT`: Set the directory where test state and test output files
should go.
`TEST_SHARED_FIXTURES`: Try to re-use a single pageserver for all the tests.
`ZENITH_PAGESERVER_OVERRIDES`: add a `;`-separated set of configs that will be passed as
`FORCE_MOCK_S3`: inits every test's pageserver with a mock S3 used as a remote storage.
`--pageserver-config-override=${value}` parameter values when zenith cli is invoked
`RUST_LOG`: logging configuration to pass into Zenith CLI

Let stdout, stderr and `INFO` log messages go to the terminal instead of capturing them:
`./scripts/pytest -s --log-cli-level=INFO ...`
(Note many tests capture subprocess outputs separately, so this may not
show much.)

Exit after the first test failure:
`./scripts/pytest -x ...`
(there are many more pytest options; run `pytest -h` to see them.)

### Writing a test

Every test needs a Zenith Environment, or ZenithEnv to operate in. A Zenith Environment
is like a little cloud-in-a-box, and consists of a Pageserver, 0-N Safekeepers, and
compute Postgres nodes. The connections between them can be configured to use JWT
authentication tokens, and some other configuration options can be tweaked too.

The easiest way to get access to a Zenith Environment is by using the `zenith_simple_env`
fixture. The 'simple' env may be shared across multiple tests, so don't shut down the nodes
or make other destructive changes in that environment. Also don't assume that
there are no tenants or branches or data in the cluster. For convenience, there is a
branch called `empty`, though. The convention is to create a test-specific branch of
that and load any test data there, instead of the 'main' branch.

For more complicated cases, you can build a custom Zenith Environment, with the `zenith_env`
fixture:

```python
def test_foobar(zenith_env_builder: ZenithEnvBuilder):
    # Prescribe the environment.
    # We want to have 3 safekeeper nodes, and use JWT authentication in the
    # connections to the page server
    zenith_env_builder.num_safekeepers = 3
    zenith_env_builder.set_pageserver_auth(True)

    # Now create the environment. This initializes the repository, and starts
    # up the page server and the safekeepers
    env = zenith_env_builder.init_start()

    # Run the test
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
