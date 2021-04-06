# Zenith

Zenith substitutes PostgreSQL storage layer and redistributes data across a cluster of nodes

## Running tests

```sh
git clone --recursive https://github.com/libzenith/zenith.git
./pgbuild.sh # builds postgres and installs it to ./tmp_install
cargo test
```

## Source tree layout

/walkeeper:

WAL safekeeper. Written in Rust.

/pageserver:

Page Server. Written in Rust.

Depends on the modified 'postgres' binary for WAL redo.

/integration_tests:

Tests with different combinations of a Postgres compute node, WAL safekeeper and Page Server.

/mgmt-console:

Web UI to launch (modified) Postgres servers, using S3 as the backing store. Written in Python.
This is somewhat outdated, as it doesn't use the WAL safekeeper or Page Servers.

/vendor/postgres:

PostgreSQL source tree, with the modifications needed for Zenith.

/vendor/postgres/src/bin/safekeeper:

Extension (safekeeper_proxy) that runs in the compute node, and connects to the WAL safekeepers
and streams the WAL



