# Command line interface (end-user)

Zenith CLI as it is described here mostly resides on the same conceptual level as pg_ctl/initdb/pg_recvxlog/etc and replaces some of them in an opinionated way. I would also suggest bundling our patched postgres inside zenith distribution at least at the start.

This proposal is focused on managing local installations. For cluster operations, different tooling would be needed. The point of integration between the two is storage URL: no matter how complex cluster setup is it may provide an endpoint where the user may push snapshots.

The most important concept here is a snapshot, which can be created/pushed/pulled/exported. Also, we may start temporary read-only postgres instance over any local snapshot. A more complex scenario would consist of several basic operations over snapshots.

# Possible usage scenarios

## Install zenith, run a postgres

```
> brew install pg-zenith 
> zenith pg create # creates pgdata with default pattern pgdata$i
> zenith pg list
ID            PGDATA        USED    STORAGE            ENDPOINT
primary1      pgdata1       0G      zenith-local       localhost:5432
```

## Import standalone postgres to zenith

```
> zenith snapshot import --from=basebackup://replication@localhost:5432/ oldpg
[====================------------] 60% | 20MB/s
> zenith snapshot list
ID          SIZE        PARENT
oldpg       5G          -

> zenith pg create --snapshot oldpg
Started postgres on localhost:5432

> zenith pg list
ID            PGDATA        USED    STORAGE            ENDPOINT
primary1      pgdata1       5G      zenith-local       localhost:5432

> zenith snapshot destroy oldpg
Ok
```

Also, we may start snapshot import implicitly by looking at snapshot schema

```
> zenith pg create --snapshot basebackup://replication@localhost:5432/
Downloading snapshot... Done.
Started postgres on localhost:5432
Destroying snapshot... Done.
```

## Pull snapshot with some publicly shared database

Since we may export the whole snapshot as one big file (tar of basebackup, maybe with some manifest) it may be shared over conventional means: http, ssh, [git+lfs](https://docs.github.com/en/github/managing-large-files/about-git-large-file-storage).

```
> zenith pg create --snapshot http://learn-postgres.com/movies_db.zenith movies
```

## Create snapshot and push it to the cloud

```
> zenith snapshot create pgdata1@snap1
> zenith snapshot push --to ssh://stas@zenith.tech pgdata1@snap1
```

## Rollback database to the snapshot

One way to rollback the database is just to init a new database from the snapshot and destroy the old one. But creating a new database from a snapshot would require a copy of that snapshot which is time consuming operation. Another option that would be cool to support is the ability to create the copy-on-write database from the snapshot without copying data, and store updated pages in a separate location, however that way would have performance implications. So to properly rollback the database to the older state we have `zenith pg checkout`.

```
> zenith pg list
ID            PGDATA        USED    STORAGE            ENDPOINT
primary1      pgdata1       5G      zenith-local       localhost:5432

> zenith snapshot create pgdata1@snap1

> zenith snapshot list
ID                    SIZE        PARENT
oldpg                 5G          -
pgdata1@snap1         6G          -
pgdata1@CURRENT       6G          -

> zenith pg checkout pgdata1@snap1
Stopping postgres on pgdata1.
Rolling back pgdata1@CURRENT to pgdata1@snap1.
Starting postgres on pgdata1.

> zenith snapshot list
ID                    SIZE        PARENT
oldpg                 5G          -
pgdata1@snap1         6G          -
pgdata1@HEAD{0}       6G          -
pgdata1@CURRENT       6G          -
```

Some notes: pgdata1@CURRENT -- implicit snapshot representing the current state of the database in the data directory. When we are checking out some snapshot CURRENT will be set to this snapshot and the old CURRENT state will be named HEAD{0} (0 is the number of postgres timeline, it would be incremented after each such checkout).

## Configure PITR area (Point In Time Recovery).

PITR area acts like a continuous snapshot where you can reset the database to any point in time within this area (by area I mean some TTL period or some size limit, both possibly infinite).

```
> zenith pitr create --storage s3tank --ttl 30d --name pitr_last_month
```

Resetting the database to some state in past would require creating a snapshot on some lsn / time in this pirt area.

# Manual

## storage

Storage is either zenith pagestore or s3. Users may create a database in a pagestore and create/move *snapshots* and *pitr regions* in both pagestore and s3. Storage is a concept similar to `git remote`. After installation, I imagine one local storage is available by default.

**zenith storage attach** -t [native|s3] -c key=value -n name

Attaches/initializes storage. For --type=s3, user credentials and path should be provided. For --type=native we may support --path=/local/path and --url=zenith.tech/stas/mystore. Other possible term for native is 'zstore'.


**zenith storage list**

Show currently attached storages. For example:

```
> zenith storage list
NAME            USED    TYPE                OPTIONS          PATH
local           5.1G    zenith-local                         /opt/zenith/store/local
local.compr     20.4G   zenith-local        comression=on    /opt/zenith/store/local.compr
zcloud          60G     zenith-remote                        zenith.tech/stas/mystore
s3tank          80G     S3
```

**zenith storage detach**

**zenith storage show**



## pg

Manages postgres data directories and can start postgreses with proper configuration. An experienced user may avoid using that (except pg create) and configure/run postgres by themself.

Pg is a term for a single postgres running on some data. I'm trying to avoid here separation of datadir management and postgres instance management -- both that concepts bundled here together.

**zenith pg create** [--no-start --snapshot --cow] -s storage-name -n pgdata

Creates (initializes) new data directory in given storage and starts postgres. I imagine that storage for this operation may be only local and data movement to remote location happens through snapshots/pitr.

--no-start: just init datadir without creating 

--snapshot snap: init from the snapshot. Snap is a name or URL (zenith.tech/stas/mystore/snap1)

--cow: initialize Copy-on-Write data directory on top of some snapshot (makes sense if it is a snapshot of currently running a database)

**zenith pg destroy**

**zenith pg start** [--replica] pgdata

Start postgres with proper extensions preloaded/installed.

**zenith pg checkout**

Rollback data directory to some previous snapshot. 

**zenith pg stop** pg_id

**zenith pg list**

```
ROLE                 PGDATA        USED    STORAGE            ENDPOINT
primary              my_pg         5.1G    local              localhost:5432
replica-1                                                     localhost:5433
replica-2                                                     localhost:5434
primary              my_pg2        3.2G    local.compr        localhost:5435
-                    my_pg3        9.2G    local.compr        -
```

**zenith pg show**

```
my_pg:
    storage: local
    space used on local: 5.1G
    space used on all storages: 15.1G
    snapshots:
        on local:
            snap1: 1G
            snap2: 1G
        on zcloud:
            snap2: 1G
        on s3tank:
            snap5: 2G
    pitr:
        on s3tank:
            pitr_one_month: 45G

```

**zenith pg start-rest/graphql** pgdata

Starts REST/GraphQL proxy on top of postgres master. Not sure we should do that, just an idea.


## snapshot

Snapshot creation is cheap -- no actual data is copied, we just start retaining old pages. Snapshot size means the amount of retained data, not all data. Snapshot name looks like pgdata_name@tag_name. tag_name is set by the user during snapshot creation. There are some reserved tag names: CURRENT represents the current state of the data directory; HEAD{i} represents the data directory state that resided in the database before i-th checkout.

**zenith snapshot create** pgdata_name@snap_name

Creates a new snapshot in the same storage where pgdata_name exists.

**zenith snapshot push** --to url pgdata_name@snap_name

Produces binary stream of a given snapshot. Under the hood starts temp read-only postgres over this snapshot and sends basebackup stream. Receiving side should start `zenith snapshot recv` before push happens. If url has some special schema like zenith:// receiving side may require auth start `zenith snapshot recv` on the go.

**zenith snapshot recv**

Starts a port listening for a basebackup stream, prints connection info to stdout (so that user may use that in push command), and expects data on that socket.

**zenith snapshot pull** --from url or path

Connects to a remote zenith/s3/file and pulls snapshot. The remote site should be zenith service or files in our format.

**zenith snapshot import** --from basebackup://<...>  or path

Creates a new snapshot out of running postgres via basebackup protocol or basebackup files.

**zenith snapshot export**

Starts read-only postgres over this snapshot and exports data in some format (pg_dump, or COPY TO on some/all tables). One of the options may be zenith own format which is handy for us (but I think just tar of basebackup would be okay).

**zenith snapshot diff** snap1 snap2

Shows size of data changed between two snapshots. We also may provide options to diff schema/data in tables. To do that start temp read-only postgreses.

**zenith snapshot destroy**

## pitr

Pitr represents wal stream and ttl policy for that stream

XXX: any suggestions on a better name?

**zenith pitr create** name

--ttl = inf | period

--size-limit = inf | limit

--storage = storage_name

**zenith pitr extract-snapshot** pitr_name --lsn xxx

Creates a snapshot out of some lsn in PITR area. The obtained snapshot may be managed with snapshot routines (move/send/export)

**zenith pitr gc** pitr_name

Force garbage collection on some PITR area.

**zenith pitr list**

**zenith pitr destroy**


## console

**zenith console**

Opens browser targeted at web console with the more or less same functionality as described here.
