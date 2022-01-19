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
- Install Python 3.7 (the minimal supported version) or greater.
    - Our setup with poetry should work with newer python versions too. So feel free to open an issue with a `c/test-runner` label if something doesnt work as expected.
    - If you have some trouble with other version you can resolve it by installing Python 3.7 separately, via pyenv or via system package manager e.g.:
      ```bash
      # In Ubuntu
      sudo add-apt-repository ppa:deadsnakes/ppa
      sudo apt update
      sudo apt install python3.7
      ```
- Install `poetry`
    - Exact version of `poetry` is not important, see installation instructions available at poetry's [website](https://python-poetry.org/docs/#installation)`.
- Install dependencies via `./scripts/pysync`. Note that CI uses Python 3.7 so if you have different version some linting tools can yield different result locally vs in the CI.

Run `poetry shell` to activate the virtual environment.
Alternatively, use `poetry run` to run a single command in the venv, e.g. `poetry run pytest`.

### Obligatory checks
We force code formatting via `yapf` and type hints via `mypy`.
Run the following commands in the repository's root (next to `setup.cfg`):

```bash
poetry run yapf -ri .  # All code is reformatted
poetry run mypy .  # Ensure there are no typing errors
```

**WARNING**: do not run `mypy` from a directory other than the root of the repository.
Otherwise it will not find its configuration.

Also consider:

* Running `flake8` (or a linter of your choice, e.g. `pycodestyle`) and fixing possible defects, if any.
* Adding more type hints to your code to avoid `Any`.

### Changing dependencies
To add new package or change an existing one you can use `poetry add` or `poetry update` or edit `pyproject.toml` manually. Do not forget to run `poetry lock` in the latter case.

More details are available in poetry's [documentation](https://python-poetry.org/docs/).
