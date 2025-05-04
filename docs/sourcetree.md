## Source tree layout

Below you will find a brief overview of each subdir in the source tree in alphabetical order.

`storage_broker`:

Neon storage broker, providing messaging between safekeepers and pageservers.
[storage_broker.md](./storage_broker.md)

`storage_controller`:

Neon storage controller, manages a cluster of pageservers and exposes an API that enables
managing a many-sharded tenant as a single entity.

`/control_plane`:

Local control plane.
Functions to start, configure and stop pageserver and postgres instances running as a local processes.
Intended to be used in integration tests and in CLI tools for local installations.

`/docs`:

Documentation of the Neon features and concepts.
Now it is mostly dev documentation.

`/pageserver`:

Neon storage service.
The pageserver has a few different duties:

- Store and manage the data.
- Generate a tarball with files needed to bootstrap ComputeNode.
- Respond to GetPage@LSN requests from the Compute Nodes.
- Receive WAL from the WAL service and decode it.
- Replay WAL that's applicable to the chunks that the Page Server maintains

For more detailed info, see [pageserver-services.md](./pageserver-services.md)

`/proxy`:

Postgres protocol proxy/router.
This service listens psql port, can check auth via external service
and create new databases and accounts (control plane API in our case).

`/test_runner`:

Integration tests, written in Python using the `pytest` framework.

`/vendor/postgres-v14` and `/vendor/postgres-v15`:

PostgreSQL source tree per version, with the modifications needed for Neon.

`/pgxn/neon`:

PostgreSQL extension that implements storage manager API and network communications with remote page server.

`/pgxn/neon_test_utils`:

PostgreSQL extension that contains functions needed for testing and debugging.

`/pgxn/neon_walredo`:

Library to run Postgres as a "WAL redo process" in the pageserver.

`/safekeeper`:

The neon WAL service that receives WAL from a primary compute nodes and streams it to the pageserver.
It acts as a holding area and redistribution center for recently generated WAL.

For more detailed info, see [walservice.md](./walservice.md)

`/workspace_hack`:
The workspace_hack crate exists only to pin down some dependencies.

We use [cargo-hakari](https://crates.io/crates/cargo-hakari) for automation.

`/libs`:
Unites granular neon helper crates under the hood.

`/libs/postgres_ffi`:

Utility functions for interacting with PostgreSQL file formats.
Misc constants, copied from PostgreSQL headers.

`/libs/utils`:
Generic helpers that are shared between other crates in this repository.
A subject for future modularization.

`/libs/metrics`:
Helpers for exposing Prometheus metrics from the server.

### Adding dependencies
When you add a Cargo dependency, you should update hakari manifest by running commands below and committing the updated `Cargo.lock` and `workspace_hack/`. There may be no changes, that's fine.

```bash
cargo hakari generate
cargo hakari manage-deps
```

If you don't have hakari installed (`error: no such subcommand: hakari`), install it by running `cargo install cargo-hakari`.

### Checking Rust 3rd-parties
[Cargo deny](https://embarkstudios.github.io/cargo-deny/index.html) is a cargo plugin that lets us lint project's dependency graph to ensure all dependencies conform to requirements. It detects security issues, matches licenses, and ensures crates only come from trusted sources.

```bash
cargo deny check
```

## Using Python
Note that Debian/Ubuntu Python packages are stale, as it commonly happens,
so manual installation of dependencies is not recommended.

A single virtual environment with all dependencies is described in the single `Pipfile`.

### Prerequisites
- Install Python 3.11 (the minimal supported version) or greater.
    - Our setup with poetry should work with newer python versions too. So feel free to open an issue with a `c/test-runner` label if something doesn't work as expected.
    - If you have some trouble with other version you can resolve it by installing Python 3.11 separately, via [pyenv](https://github.com/pyenv/pyenv) or via system package manager e.g.:
      ```bash
      # In Ubuntu
      sudo add-apt-repository ppa:deadsnakes/ppa
      sudo apt update
      sudo apt install python3.11
      ```
- Install `poetry`
    - Exact version of `poetry` is not important, see installation instructions available at poetry's [website](https://python-poetry.org/docs/#installation).
- Install dependencies via `./scripts/pysync`.
    - Note that CI uses specific Python version (look for `PYTHON_VERSION` [here](https://github.com/neondatabase/docker-images/blob/main/rust/Dockerfile))
      so if you have different version some linting tools can yield different result locally vs in the CI.
    - You can explicitly specify which Python to use by running `poetry env use /path/to/python`, e.g. `poetry env use python3.11`.
      This may also disable the `The currently activated Python version X.Y.Z is not supported by the project` warning.

Run `poetry shell` to activate the virtual environment.
Alternatively, use `poetry run` to run a single command in the venv, e.g. `poetry run pytest`.

### Obligatory checks
We force code formatting via `ruff`, and type hints via `mypy`.
Run the following commands in the repository's root (next to `pyproject.toml`):

```bash
poetry run ruff format . # All code is reformatted
poetry run ruff check .  # Python linter
poetry run mypy .        # Ensure there are no typing errors
```

**WARNING**: do not run `mypy` from a directory other than the root of the repository.
Otherwise it will not find its configuration.

Also consider:

* Running `pycodestyle` (or a linter of your choice) and fixing possible defects, if any.
* Adding more type hints to your code to avoid `Any`.

### Changing dependencies
To add new package or change an existing one you can use `poetry add` or `poetry update` or edit `pyproject.toml` manually. Do not forget to run `poetry lock` in the latter case.

More details are available in poetry's [documentation](https://python-poetry.org/docs/).

## Configuring IDEs
Neon consists of three projects in different languages which use different project models.

* A bunch of Rust crates, all available from the root `Cargo.toml`.
* Integration tests in Python in the `test_runner` directory. Some stand-alone Python scripts exist as well.
* Postgres and our Postgres extensions in C built with Makefiles under `vendor/postgres` and `pgxn`.

### CLion
You can use CLion with the [Rust plugin](https://plugins.jetbrains.com/plugin/8182-rust) to develop Neon. It should pick up Rust and Python projects whenever you open Neon's repository as a project. We have not tried setting up a debugger, though.

C code requires some extra care, as it's built via Make, not CMake. Some of our developers have successfully used [compilation database](https://www.jetbrains.com/help/clion/compilation-database.html#compdb_generate) for CLion. It is a JSON file which lists all C source files and corresponding compilation keys. CLion can use it instead of `CMakeLists.txt`. To set up a project with a compilation database:

1. Clone the Neon repository and install all dependencies, including Python. Do not open it with CLion just yet.
2. Run the following commands in the repository's root:
   ```bash
   # Install a `compiledb` tool which can parse make's output and generate the compilation database.
   poetry add -D compiledb
   # Clean the build tree so we can rebuild from scratch.
   # Unfortunately, our and Postgres Makefiles do not work well with either --dry-run or --assume-new,
   # so we don't know a way to generate the compilation database without recompiling everything,
   # see https://github.com/neondatabase/neon/issues/2378#issuecomment-1241421325
   make distclean
   # Rebuild the Postgres parts from scratch and save the compilation commands to the compilation database.
   # You can alter the -j parameter to your liking.
   # Note that we only build for a specific version of Postgres. The extension code is shared, but headers are
   # different, so we set up CLion to only use a specific version of the headers.
   make -j$(nproc) --print-directory postgres-v15 neon-pg-ext-v15 | poetry run compiledb --verbose --no-build
   # Uninstall the tool
   poetry remove -D compiledb
   # Make sure the compile_commands.json file is not committed.
   echo /compile_commands.json >>.git/info/exclude
   ```
3. Open CLion, click "Open File or Project" and choose the generated `compile_commands.json` file to be opened "as a project". You cannot add a compilation database into an existing CLion project, you have to create a new one. _Do not_ open the directory as a project, open the file.
4. The newly created project should start indexing Postgres source code in C, as well as the C standard library. You may have to [configure the C compiler for the compilation database](https://www.jetbrains.com/help/clion/compilation-database.html#compdb_toolchain).
5. Open the `Cargo.toml` file in an editor in the same project. CLion should pick up the hint and start indexing Rust code.
6. Now you have a CLion project which knows about C files, Rust files. It should pick up Python files automatically as well.
7. Set up correct code indentation in CLion's settings: Editor > Code Style > C/C++, choose the "Project" scheme on the top, and tick the "Use tab character" on the "Tabs and Indents" tab. Ensure that "Tab size" is 4.

You can also enable Cargo Clippy diagnostics and enable Rustfmt instead of built-in code formatter.

Whenever you change layout of C files, you may need to regenerate the compilation database. No need to re-create the CLion project, changes should be picked up automatically.

Known issues (fixes and suggestions are welcome):

* Test results may be hard to read in CLion, both for unit tests in Rust and integration tests in Python. Use command line to run them instead.
* CLion does not support non-local Python interpreters, unlike PyCharm. E.g. if you use WSL, CLion does not see `poetry` and installed dependencies. Python support is limited.
* Cargo Clippy diagnostics in CLion may take a lot of resources.
* `poetry add -D` updates some packages and changes `poetry.lock` drastically even when followed by `poetry remove -D`. Feel free to `git checkout poetry.lock` and `./scripts/pysync` to revert these changes.
