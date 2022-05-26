# Neon

Neon is a serverless open source alternative to AWS Aurora Postgres. It separates storage and compute and substitutes PostgreSQL storage layer by redistributing data across a cluster of nodes.

The project used to be called "Zenith". Many of the commands and code comments
still refer to "zenith", but we are in the process of renaming things.

## Quick start
[Join the waitlist](https://neon.tech/) for our free tier to receive your serverless postgres instance. Then connect to it with your preferred postgres client (psql, dbeaver, etc) or use the online SQL editor.

Alternatively, compile and run the project [locally](#running-local-installation).

## Architecture overview

A Neon installation consists of compute nodes and Neon storage engine.

Compute nodes are stateless PostgreSQL nodes, backed by Neon storage engine.

Neon storage engine consists of two major components:
- Pageserver. Scalable storage backend for compute nodes.
- WAL service. The service that receives WAL from compute node and ensures that it is stored durably.

Pageserver consists of:
- Repository - Neon storage implementation.
- WAL receiver - service that receives WAL from WAL service and stores it in the repository.
- Page service - service that communicates with compute nodes and responds with pages from the repository.
- WAL redo - service that builds pages from base images and WAL records on Page service request.

## Running local installation


#### building on Ubuntu/ Debian (Linux)
1. Install build dependencies and other useful packages

On Ubuntu or Debian this set of packages should be sufficient to build the code:
```text
apt install build-essential libtool libreadline-dev zlib1g-dev flex bison libseccomp-dev \
libssl-dev clang pkg-config libpq-dev libprotobuf-dev etcd
```

2. [Install Rust](https://www.rust-lang.org/tools/install)
```
# recommended approach from https://www.rust-lang.org/tools/install
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

3. Install PostgreSQL Client
```
apt install postgresql-client
```

4. Build neon and patched postgres
```sh
git clone --recursive https://github.com/neondatabase/neon.git
cd neon
make -j5
```

#### building on OSX (12.3.1)
1. Install XCode and dependencies
```
xcode-select --install
brew install protobuf etcd
```

2. [Install Rust](https://www.rust-lang.org/tools/install)
```
# recommended approach from https://www.rust-lang.org/tools/install
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

3. Install PostgreSQL Client
```
# from https://stackoverflow.com/questions/44654216/correct-way-to-install-psql-without-full-postgres-on-macos
brew install libpq
brew link --force libpq
```

4. Build neon and patched postgres
```sh
git clone --recursive https://github.com/neondatabase/neon.git
cd neon
make -j5
```

#### dependency installation notes
To run the `psql` client, install the `postgresql-client` package or modify `PATH` and `LD_LIBRARY_PATH` to include `tmp_install/bin` and `tmp_install/lib`, respectively.

To run the integration tests or Python scripts (not required to use the code), install
Python (3.9 or higher), and install python3 packages using `./scripts/pysync` (requires poetry) in the project directory.


#### running neon database
1. Start pageserver and postgres on top of it (should be called from repo root):
```sh
# Create repository in .zenith with proper paths to binaries and data
# Later that would be responsibility of a package install script
> ./target/debug/neon_local init
initializing tenantid 9ef87a5bf0d92544f6fafeeb3239695c
created initial timeline de200bd42b49cc1814412c7e592dd6e9 timeline.lsn 0/16B5A50
initial timeline de200bd42b49cc1814412c7e592dd6e9 created
pageserver init succeeded

# start pageserver and safekeeper
> ./target/debug/neon_local start
Starting pageserver at '127.0.0.1:64000' in '.zenith'
Pageserver started
initializing for sk 1 for 7676
Starting safekeeper at '127.0.0.1:5454' in '.zenith/safekeepers/sk1'
Safekeeper started

# start postgres compute node
> ./target/debug/neon_local pg start main
Starting new postgres main on timeline de200bd42b49cc1814412c7e592dd6e9 ...
Extracting base backup to create postgres instance: path=.zenith/pgdatadirs/tenants/9ef87a5bf0d92544f6fafeeb3239695c/main port=55432
Starting postgres node at 'host=127.0.0.1 port=55432 user=cloud_admin dbname=postgres'

# check list of running postgres instances
> ./target/debug/neon_local pg list
 NODE  ADDRESS          TIMELINE                          BRANCH NAME  LSN        STATUS
 main  127.0.0.1:55432  de200bd42b49cc1814412c7e592dd6e9  main         0/16B5BA8  running
```

2. Now it is possible to connect to postgres and run some queries:
```text
> psql -p55432 -h 127.0.0.1 -U cloud_admin postgres
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

3. And create branches and run postgres on them:
```sh
# create branch named migration_check
> ./target/debug/neon_local timeline branch --branch-name migration_check
Created timeline 'b3b863fa45fa9e57e615f9f2d944e601' at Lsn 0/16F9A00 for tenant: 9ef87a5bf0d92544f6fafeeb3239695c. Ancestor timeline: 'main'

# check branches tree
> ./target/debug/neon_local timeline list
(L) main [de200bd42b49cc1814412c7e592dd6e9]
(L) ┗━ @0/16F9A00: migration_check [b3b863fa45fa9e57e615f9f2d944e601]

# start postgres on that branch
> ./target/debug/neon_local pg start migration_check --branch-name migration_check
Starting new postgres migration_check on timeline b3b863fa45fa9e57e615f9f2d944e601 ...
Extracting base backup to create postgres instance: path=.zenith/pgdatadirs/tenants/9ef87a5bf0d92544f6fafeeb3239695c/migration_check port=55433
Starting postgres node at 'host=127.0.0.1 port=55433 user=cloud_admin dbname=postgres'

# check the new list of running postgres instances
> ./target/debug/neon_local pg list
 NODE             ADDRESS          TIMELINE                          BRANCH NAME      LSN        STATUS
 main             127.0.0.1:55432  de200bd42b49cc1814412c7e592dd6e9  main             0/16F9A38  running
 migration_check  127.0.0.1:55433  b3b863fa45fa9e57e615f9f2d944e601  migration_check  0/16F9A70  running

# this new postgres instance will have all the data from 'main' postgres,
# but all modifications would not affect data in original postgres
> psql -p55433 -h 127.0.0.1 -U cloud_admin postgres
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)

postgres=# insert into t values(2,2);
INSERT 0 1

# check that the new change doesn't affect the 'main' postgres
> psql -p55432 -h 127.0.0.1 -U cloud_admin postgres
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)
```

4. If you want to run tests afterwards (see below), you have to stop all the running the pageserver, safekeeper and postgres instances
   you have just started. You can stop them all with one command:
```sh
> ./target/debug/neon_local stop
```

## Running tests

```sh
git clone --recursive https://github.com/neondatabase/neon.git
make # builds also postgres and installs it to ./tmp_install
./scripts/pytest
```

## Documentation

Now we use README files to cover design ideas and overall architecture for each module and `rustdoc` style documentation comments. See also [/docs/](/docs/) a top-level overview of all available markdown documentation.

- [/docs/sourcetree.md](/docs/sourcetree.md) contains overview of source tree layout.

To view your `rustdoc` documentation in a browser, try running `cargo doc --no-deps --open`

### Postgres-specific terms

Due to Neon's very close relation with PostgreSQL internals, there are numerous specific terms used.
Same applies to certain spelling: i.e. we use MB to denote 1024 * 1024 bytes, while MiB would be technically more correct, it's inconsistent with what PostgreSQL code and its documentation use.

To get more familiar with this aspect, refer to:

- [Neon glossary](/docs/glossary.md)
- [PostgreSQL glossary](https://www.postgresql.org/docs/13/glossary.html)
- Other PostgreSQL documentation and sources (Neon fork sources can be found [here](https://github.com/neondatabase/postgres))

## Join the development

- Read `CONTRIBUTING.md` to learn about project code style and practices.
- To get familiar with a source tree layout, use [/docs/sourcetree.md](/docs/sourcetree.md).
- To learn more about PostgreSQL internals, check http://www.interdb.jp/pg/index.html
