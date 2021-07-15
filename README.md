# Zenith

Zenith substitutes PostgreSQL storage layer and redistributes data across a cluster of nodes

## Running local installation

1. Install build dependencies and other useful packages

On Ubuntu or Debian this set of packages should be sufficient to build the code:
```text
apt install build-essential libtool libreadline-dev zlib1g-dev flex bison libseccomp-dev \
libssl-dev clang
```

[Rust] 1.48 or later is also required.

To run the `psql` client, install the `postgresql-client` package or modify `PATH` and `LD_LIBRARY_PATH` to include `tmp_install/bin` and `tmp_install/lib`, respectively.

To run the integration tests (not required to use the code), install
Python (3.6 or higher), and install python3 packages with `pip` (called `pip3` on some systems):
```
pip install pytest psycopg2
```

2. Build zenith and patched postgres
```sh
git clone --recursive https://github.com/zenithdb/zenith.git
cd zenith
make -j5
```

3. Start pageserver and postgres on top of it (should be called from repo root):
```sh
# Create repository in .zenith with proper paths to binaries and data
# Later that would be responsibility of a package install script
> ./target/debug/zenith init
pageserver init succeeded

# start pageserver
> ./target/debug/zenith start
Starting pageserver at '127.0.0.1:64000' in .zenith
Pageserver started

# start postgres on top on the pageserver
> ./target/debug/zenith pg start main
Starting postgres node at 'host=127.0.0.1 port=55432 user=stas'
waiting for server to start.... done

# check list of running postgres instances
> ./target/debug/zenith pg list
BRANCH	ADDRESS		LSN		STATUS
main	127.0.0.1:55432	0/1609610	running
```

4. Now it is possible to connect to postgres and run some queries:
```text
> psql -p55432 -h 127.0.0.1 postgres
postgres=# CREATE TABLE t(key int primary key, value text);
CREATE TABLE
postgres=# insert into t values(1,1);
INSERT 0 1
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)
```

5. And create branches and run postgres on them:
```sh
# create branch named migration_check
> ./target/debug/zenith branch migration_check main
Created branch 'migration_check' at 0/1609610

# check branches tree
> ./target/debug/zenith branch
 main
 ┗━ @0/1609610: migration_check

# start postgres on that branch
> ./target/debug/zenith pg start migration_check
Starting postgres node at 'host=127.0.0.1 port=55433 user=stas'
waiting for server to start.... done

# this new postgres instance will have all the data from 'main' postgres,
# but all modifications would not affect data in original postgres
> psql -p55433 -h 127.0.0.1 postgres
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)

postgres=# insert into t values(2,2);
INSERT 0 1
```

## Running tests

```sh
git clone --recursive https://github.com/libzenith/zenith.git
make # builds also postgres and installs it to ./tmp_install
cd test_runner
pytest
```

## Documentation

Now we use README files to cover design ideas and overall architecture for each module.
And rustdoc style documentation comments.

To view your documentation in a browser, try running `cargo doc --no-deps --open`

## Source tree layout

`/control_plane`:

Local control plane.
Functions to start, configure and stop pageserver and postgres instances running as a local processes.
Intended to be used in integration tests and in CLI tools for local installations.

`/zenith`

Main entry point for the 'zenith' CLI utility.
TODO: Doesn't it belong to control_plane?

`/postgres_ffi`:

Utility functions for interacting with PostgreSQL file formats.
Misc constants, copied from PostgreSQL headers.

`/zenith_utils`:

Helpers that are shared between other crates in this repository.

`/walkeeper`:

WAL safekeeper (also known as WAL acceptor). Written in Rust.

`/pageserver`:

Page Server. Written in Rust.

Depends on the modified 'postgres' binary for WAL redo.

`/vendor/postgres`:

PostgreSQL source tree, with the modifications needed for Zenith.

`/vendor/postgres/contrib/zenith`:

PostgreSQL extension that implements storage manager API and network communications with remote page server.

`/test_runner`:

Integration tests, written in Python using the `pytest` framework.

`test_runner/zenith_regress`:

Quick way to add new SQL regression test to integration tests set.
