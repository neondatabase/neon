# Running neon Database

## Init neon

Call from Repository root.

```bash
# Create repository in .neon with proper paths to binaries and data
# Later that would be responsibility of a package install script
> cargo neon init
Initializing pageserver node 1 at '127.0.0.1:64000' in ".neon"
```

## Start neon

Start pageserver, safekeeper, and broker for their intercommunication.

```bash
> cargo neon start
Starting neon broker at 127.0.0.1:50051.
storage_broker started, pid: 2918372
Starting pageserver node 1 at '127.0.0.1:64000' in ".neon".
pageserver started, pid: 2918386
Starting safekeeper at '127.0.0.1:5454' in '.neon/safekeepers/sk1'.
safekeeper 1 started, pid: 2918437
```

## Create initial tenant

Also set --set-default flag for every future neon_local invocation.

```bash
> cargo neon tenant create --set-default
tenant 9ef87a5bf0d92544f6fafeeb3239695c successfully created on the pageserver
Created an initial timeline 'de200bd42b49cc1814412c7e592dd6e9' at Lsn 0/16B5A50 for tenant: 9ef87a5bf0d92544f6fafeeb3239695c
Setting tenant 9ef87a5bf0d92544f6fafeeb3239695c as a default one

# create postgres compute node
> cargo neon endpoint create main

# start postgres compute node
> cargo neon endpoint start main
Starting new endpoint main (PostgreSQL v14) on timeline de200bd42b49cc1814412c7e592dd6e9 ...
Starting postgres at 'postgresql://cloud_admin@127.0.0.1:55432/postgres'

# check list of running postgres instances
> cargo neon endpoint list
 ENDPOINT  ADDRESS          TIMELINE                          BRANCH NAME  LSN        STATUS
 main      127.0.0.1:55432  de200bd42b49cc1814412c7e592dd6e9  main         0/16B5BA8  running
```

## Check psql Connection

Now it is possible to connect to postgres and run some queries:

```sql
> psql -p 55432 -h 127.0.0.1 -U cloud_admin postgres
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

## Create Branches

3. And create branches and run postgres on them:
```bash
# create branch named migration_check
> cargo neon timeline branch --branch-name migration_check
Created timeline 'b3b863fa45fa9e57e615f9f2d944e601' at Lsn 0/16F9A00 for tenant: 9ef87a5bf0d92544f6fafeeb3239695c. Ancestor timeline: 'main'

# check branches tree
> cargo neon timeline list
(L) main [de200bd42b49cc1814412c7e592dd6e9]
(L) ┗━ @0/16F9A00: migration_check [b3b863fa45fa9e57e615f9f2d944e601]

# create postgres on that branch
> cargo neon endpoint create migration_check --branch-name migration_check

# start postgres on that branch
> cargo neon endpoint start migration_check
Starting new endpoint migration_check (PostgreSQL v14) on timeline b3b863fa45fa9e57e615f9f2d944e601 ...
Starting postgres at 'postgresql://cloud_admin@127.0.0.1:55434/postgres'

# check the new list of running postgres instances
> cargo neon endpoint list
 ENDPOINT         ADDRESS          TIMELINE                          BRANCH NAME      LSN        STATUS
 main             127.0.0.1:55432  de200bd42b49cc1814412c7e592dd6e9  main             0/16F9A38  running
 migration_check  127.0.0.1:55434  b3b863fa45fa9e57e615f9f2d944e601  migration_check  0/16F9A70  running

# this new postgres instance will have all the data from 'main' postgres,
# but all modifications would not affect data in original postgres
> psql -p 55434 -h 127.0.0.1 -U cloud_admin postgres
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)

postgres=# insert into t values(2,2);
INSERT 0 1

# check that the new change doesn't affect the 'main' postgres
> psql -p 55432 -h 127.0.0.1 -U cloud_admin postgres
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)
```

## Handling Failures

If you encounter errors during setting up the initial tenant, it's best to stop everything (`cargo neon stop`) and
remove the `.neon` directory. Then fix the problems, and start the setup again.

# Advanced Usages

More advanced usages can be found at [Control Plane and Neon Local](./control_plane/README.md).
