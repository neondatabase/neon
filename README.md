# Zenith

Zenith substitutes PostgreSQL storage layer and redistributes data across a cluster of nodes

## Running local installation

1. Build zenith and patched postgres
```sh
git clone --recursive https://github.com/libzenith/zenith.git
cd zenith
make -j5
```

2. Start pageserver and postgres on top of it (should be called from repo root):
```sh
# Create repository in .zenith with proper paths to binaries and data
# Later that would be responsibility of a package install script
> ./target/debug/zenith init
<...>
new zenith repository was created in .zenith

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

3. Now it is possible to connect to postgres and run some queries:
```sh
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

4. And create branches and run postgres on them:
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
pytest
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



