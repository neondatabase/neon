## Zenith test runner

This directory contains integration tests.

Prerequisites:
- Python 3.6 or later
- Dependencies: install them via `pipenv install`. Note that Debian/Ubuntu
  packages are stale, as it commonly happens, so manual installation is not
  recommended.
  Run `pipenv shell` to activate the venv or use `pipenv run` to run a single
  command in the venv, e.g. `pipenv run pytest`.
- Zenith and Postgres binaries
    - See the root README.md for build directions
    - Tests can be run from the git tree; or see the environment variables
      below to run from other directories.
- The zenith git repo, including the postgres submodule
  (for some tests, e.g. pg_regress)

### Test Organization

The tests are divided into a few batches, such that each batch takes roughly
the same amount of time. The batches can be run in parallel, to minimize total
runtime. Currently, there are only two batches:

- test_batch_pg_regress: Runs PostgreSQL regression tests
- test_others: All other tests

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
`POSTGRES_DISTRIB_DIR`: The directory where postgres distribution can be found.
`TEST_OUTPUT`: Set the directory where test state and test output files
should go.
`TEST_SHARED_FIXTURES`: Try to re-use a single pageserver for all the tests.

Let stdout, stderr and `INFO` log messages go to the terminal instead of capturing them:
`pytest -s --log-cli-level=INFO ...`
(Note many tests capture subprocess outputs separately, so this may not
show much.)

Exit after the first test failure:
`pytest -x ...`
(there are many more pytest options; run `pytest -h` to see them.)


### Building new tests

The tests make heavy use of pytest fixtures. You can read about how they work here: https://docs.pytest.org/en/stable/fixture.html

Essentially, this means that each time you see a fixture named as an input parameter, the function with that name will be run and passed as a parameter to the function.

So this code:

```python
def test_something(zenith_cli, pg_bin):
    pass
```

... will run the fixtures called `zenith_cli` and `pg_bin` and deliver those results to the test function.

Fixtures can't be imported using the normal python syntax. Instead, use this:

```python
pytest_plugins = ("fixtures.something")
```

That will make all the fixtures in the `fixtures/something.py` file available.

Anything that's likely to be used in multiple tests should be built into a fixture.

Note that fixtures can clean up after themselves if they use the `yield` syntax.
Cleanup will happen even if the test fails (raises an unhandled exception).
Python destructors, e.g. `__del__()` aren't recommended for cleanup.


### Code quality

We force code formatting via yapf:

1. Install `yapf` and other tools (`flake8`, `mypy`) with `pipenv install --dev`.
1. Reformat all your code by running `pipenv run yapf -ri .` in the `test_runner/` directory.

Before submitting a patch, please consider:

* Writing a couple of docstrings to clarify the reasoning behind a new test.
* Running `flake8` (or a linter of your choice, e.g. `pycodestyle`) and fixing possible defects, if any.
* (Optional) Typechecking the code with `mypy .`. Currently this mostly affects `fixtures/zenith_fixtures.py`.
