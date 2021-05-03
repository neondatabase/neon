## Zenith test runner

This directory contains integration tests.

Prerequisites:
- Python 3.6 or later
- Python packages: pytest, psycopg2
    - pytest 6.0 is required.
    - __NOTE: `apt install` on Debian/Ubuntu won't work.__
      They ship a much older version of pytest (and sometimes rename it to
      `pytest-3`.)
    - Install using something like this:
        - `pip3 install pytest psycopg2` (Debian or Ubuntu)
- Zenith and Postgres binaries
    - See the root README.md for build directions
    - Tests can be run from the git tree; or see the environment variables
      below to run from other directories.
- The zenith git repo, including the postgres submodule
  (for some tests, e.g. pg_regress)

### Running the tests

Because pytest will search all subdirectories for tests, it's easiest to
run the tests from within the `test_runner` directory.

Test state (postgres data, pageserver state, and log files) will
be stored under a directory `test_output`.

You can run all the tests with:

`pytest`

If you want to run all the tests in a particular file:

`pytest test_pgbench.py`

If you want to run all tests that have the string "bench" in their names:

`pytest -k bench`

Useful environment variables:

`ZENITH_BIN`: The directory where zenith binaries can be found.
`POSTGRES_BIN`: The directory where postgres binaries can be found.
`TEST_OUTPUT`: Set the directory where test state and test output files
should go.
`TEST_SHARED_FIXTURES`: Try to re-use a single postgres and pageserver
for all the tests.

Let stdout and stderr go to the terminal instead of capturing them:
`pytest -s ...`
(Note many tests capture subprocess outputs separately, so this may not
show much.)

Exit after the first test failure:
`pytest -x ...`
(there are many more pytest options; run `pytest -h` to see them.)


### Building new tests

The tests make heavy use of pytest fixtures. You can read about how they work here: https://docs.pytest.org/en/stable/fixture.html

Essentially, this means that each time you see a fixture named as an input parameter, the function with that name will be run and passed as a parameter to the function.

So this code:
```
def test_something(zenith_cli, pg_bin):
    pass
```

... will run the fixtures called `zenith_cli` and `pg_bin` and deliver those results to the test function.

Fixtures can't be imported using the normal python syntax. Instead, use this:
```
pytest_plugins = ("fixtures.something")
```
That will make all the fixtures in the `fixtures/something.py` file available.

Anything that's likely to be used in multiple tests should be built into a fixture.

Note that fixtures can clean up after themselves if they use the `yield` syntax.
Cleanup will happen even if the test fails (raises an unhandled exception).
Python destructors, e.g. `__del__()` aren't recommended for cleanup.
