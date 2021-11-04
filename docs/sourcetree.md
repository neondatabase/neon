## Source tree layout

Below you will find a brief overview of each subdir in the source tree in alphabetical order.

`/control_plane`:

Local control plane.
Functions to start, configure and stop pageserver and postgres instances running as a local processes.
Intended to be used in integration tests and in CLI tools for local installations.

`/docs`:

Documentaion of the Zenith features and concepts.
Now it is mostly dev documentation.

`/monitoring`:

TODO

`/pageserver`:

Zenith storage service.
The pageserver has a few different duties:

- Store and manage the data.
- Generate a tarball with files needed to bootstrap ComputeNode.
- Respond to GetPage@LSN requests from the Compute Nodes.
- Receive WAL from the WAL service and decode it.
- Replay WAL that's applicable to the chunks that the Page Server maintains

For more detailed info, see `/pageserver/README`

`/postgres_ffi`:

Utility functions for interacting with PostgreSQL file formats.
Misc constants, copied from PostgreSQL headers.

`/proxy`:

Postgres protocol proxy/router.
This service listens psql port, can check auth via external service
and create new databases and accounts (control plane API in our case).

`/test_runner`:

Integration tests, written in Python using the `pytest` framework.

`/vendor/postgres`:

PostgreSQL source tree, with the modifications needed for Zenith.

`/vendor/postgres/contrib/zenith`:

PostgreSQL extension that implements storage manager API and network communications with remote page server.

`/vendor/postgres/contrib/zenith_test_utils`:

PostgreSQL extension that contains functions needed for testing and debugging.

`/walkeeper`:

The zenith WAL service that receives WAL from a primary compute nodes and streams it to the pageserver.
It acts as a holding area and redistribution center for recently generated WAL.

For more detailed info, see `/walkeeper/README`

`/workspace_hack`:
The workspace_hack crate exists only to pin down some dependencies.

`/zenith`

Main entry point for the 'zenith' CLI utility.
TODO: Doesn't it belong to control_plane?

`/zenith_metrics`:

Helpers for exposing Prometheus metrics from the server.

`/zenith_utils`:

Helpers that are shared between other crates in this repository.

## Using Python
Note that Debian/Ubuntu Python packages are stale, as it commonly happens,
so manual installation of dependencies is not recommended.

A single virtual environment with all dependencies is described in the single `Pipfile`.

### Prerequisites
- Install Python 3.7 (the minimal supported version)
    - Later version (e.g. 3.8) is ok if you don't write Python code
    - You can install Python 3.7 separately, e.g.:
      ```bash
      # In Ubuntu
      sudo add-apt-repository ppa:deadsnakes/ppa
      sudo apt update
      sudo apt install python3.7
      ```
- Install `pipenv`
    - Exact version of `pipenv` is not important, you can use Debian/Ubuntu package `pipenv`.
- Install dependencies via either
  * `pipenv --python 3.7 install --dev` if you will write Python code, or
  * `pipenv install` if you only want to run Python scripts and don't have Python 3.7.

Run `pipenv shell` to activate the virtual environment.
Alternatively, use `pipenv run` to run a single command in the venv, e.g. `pipenv run pytest`.

### Obligatory checks
We force code formatting via `yapf` and type hints via `mypy`.
Run the following commands in the repository's root (next to `setup.cfg`):

```bash
pipenv run yapf -ri .  # All code is reformatted
pipenv run mypy .  # Ensure there are no typing errors
```

**WARNING**: do not run `mypy` from a directory other than the root of the repository.
Otherwise it will not find its configuration.

Also consider:

* Running `flake8` (or a linter of your choice, e.g. `pycodestyle`) and fixing possible defects, if any.
* Adding more type hints to your code to avoid `Any`.

### Changing dependencies
You have to update `Pipfile.lock` if you have changed `Pipfile`:

```bash
pipenv --python 3.7 install --dev  # Re-create venv for Python 3.7 and install recent pipenv inside
pipenv run pipenv --version  # Should be at least 2021.5.29
pipenv run pipenv lock  # Regenerate Pipfile.lock
```

As the minimal supported version is Python 3.7 and we use it in CI,
you have to use a Python 3.7 environment when updating `Pipfile.lock`.
Otherwise some back-compatibility packages will be missing.

It is also important to run recent `pipenv`.
Older versions remove markers from `Pipfile.lock`.
