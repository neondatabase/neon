# Zenith

Zenith substitutes PostgreSQL storage layer and redistributes data across a cluster of nodes

## Running local installation

1. Build zenith and patched postgres
```sh
git clone --recursive https://github.com/libzenith/zenith.git
cd zenith
make
```

2. Start pageserver and postggres on top of it (should be called from repo root):
```sh
# Create ~/.zenith with proper paths to binaries and data
# Later that would be responsibility of a package install script
>./target/debug/zenith init

# start pageserver
> ./target/debug/zenith pageserver start
Starting pageserver at '127.0.0.1:64000'

# create and configure postgres data dir
> ./target/debug/zenith pg create
Creating new postgres: path=/Users/user/code/zenith/tmp_check_cli/compute/pg1 port=55432
Database initialized

# start it
> ./target/debug/zenith pg start pg1

# look up status and connection info
> ./target/debug/zenith pg list     
NODE		ADDRESS				STATUS
pg1			127.0.0.1:55432		running
```

3. Now it is possible to connect to postgres and run some queries:
```
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

## Running tests

```sh
git clone --recursive https://github.com/libzenith/zenith.git
make # builds also postgres and installs it to ./tmp_install
cargo test -- --test-threads=1
```

## Source tree layout

/walkeeper:

WAL safekeeper. Written in Rust.

/pageserver:

Page Server. Written in Rust.

Depends on the modified 'postgres' binary for WAL redo.

/integration_tests:

Tests with different combinations of a Postgres compute node, WAL safekeeper and Page Server.

/vendor/postgres:

PostgreSQL source tree, with the modifications needed for Zenith.

/vendor/postgres/src/bin/safekeeper:

Extension (safekeeper_proxy) that runs in the compute node, and connects to the WAL safekeepers
and streams the WAL



