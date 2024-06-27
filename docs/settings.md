## Pageserver

Pageserver is mainly configured via a `pageserver.toml` config file.
If there's no such file during `init` phase of the server, it creates the file itself. Without 'init', the file is read.

There's a possibility to pass an arbitrary config value to the pageserver binary as an argument: such values override
the values in the config file, if any are specified for the same key and get into the final config during init phase.

### Config example

```toml
# Initial configuration file created by 'pageserver --init'
listen_pg_addr = '127.0.0.1:64000'
listen_http_addr = '127.0.0.1:9898'

checkpoint_distance = '268435456' # in bytes
checkpoint_timeout = '10m'

gc_period = '1 hour'
gc_horizon = '67108864'

max_file_descriptors = '100'

# initial superuser role name to use when creating a new tenant
initial_superuser_name = 'cloud_admin'

broker_endpoint = 'http://127.0.0.1:50051'

# [remote_storage]
```

The config above shows default values for all basic pageserver settings, besides `broker_endpoint`: that one has to be set by the user,
see the corresponding section below.
Pageserver uses default values for all files that are missing in the config, so it's not a hard error to leave the config blank.
Yet, it validates the config values it can (e.g. postgres install dir) and errors if the validation fails, refusing to start.

Note the `[remote_storage]` section: it's a [table](https://toml.io/en/v1.0.0#table) in TOML specification and

- either has to be placed in the config after the table-less values such as `initial_superuser_name = 'cloud_admin'`

- or can be placed anywhere if rewritten in identical form as [inline table](https://toml.io/en/v1.0.0#inline-table): `remote_storage = {foo = 2}`

### Config values

All values can be passed as an argument to the pageserver binary, using the `-c` parameter and specified as a valid TOML string. All tables should be passed in the inline form.

Example: `${PAGESERVER_BIN} -c "checkpoint_timeout = '10 m'" -c "remote_storage={local_path='/some/local/path/'}"`

Note that TOML distinguishes between strings and integers, the former require single or double quotes around them.

#### broker_endpoint

A storage broker endpoint to connect and pull the information from. Default is
`'http://127.0.0.1:50051'`. 

#### checkpoint_distance

`checkpoint_distance` is the amount of incoming WAL that is held in
the open layer, before it's flushed to local disk. It puts an upper
bound on how much WAL needs to be re-processed after a pageserver
crash. It is a soft limit, the pageserver can momentarily go above it,
but it will trigger a checkpoint operation to get it back below the
limit.

`checkpoint_distance` also determines how much WAL needs to be kept
durable in the safekeeper. The safekeeper must have capacity to hold
this much WAL, with some headroom, otherwise you can get stuck in a
situation where the safekeeper is full and stops accepting new WAL,
but the pageserver is not flushing out and releasing the space in the
safekeeper because it hasn't reached checkpoint_distance yet.

`checkpoint_distance` also controls how often the WAL is uploaded to
S3.

The unit is # of bytes.

#### checkpoint_timeout

Apart from `checkpoint_distance`, open layer flushing is also triggered
`checkpoint_timeout` after the last flush. This makes WAL eventually uploaded to
s3 when activity is stopped.

The default is 10m.

#### compaction_period

Every `compaction_period` seconds, the page server checks if
maintenance operations, like compaction, are needed on the layer
files. Default is 1 s, which should be fine.

#### compaction_target_size

File sizes for L0 delta and L1 image layers. Default is 128MB.

#### gc_horizon

`gz_horizon` determines how much history is retained, to allow
branching and read replicas at an older point in time. The unit is #
of bytes of WAL. Page versions older than this are garbage collected
away.

#### gc_period

Interval at which garbage collection is triggered. Default is 1 hour.

#### image_creation_threshold

L0 delta layer threshold for L1 image layer creation. Default is 3.

#### pitr_interval

WAL retention duration for PITR branching. Default is 7 days.

#### walreceiver_connect_timeout

Time to wait to establish the wal receiver connection before failing

#### lagging_wal_timeout

Time the pageserver did not get any WAL updates from safekeeper (if any).
Avoids lagging pageserver preemptively by forcing to switch it from stalled connections.

#### max_lsn_wal_lag

Difference between Lsn values of the latest available WAL on safekeepers: if currently connected safekeeper starts to lag too long and too much,
it gets swapped to the different one.

#### initial_superuser_name

Name of the initial superuser role, passed to initdb when a new tenant
is initialized. It doesn't affect anything after initialization. The
default is Note: The default is 'cloud_admin', and the console
depends on that, so if you change it, bad things will happen.

#### page_cache_size

Size of the page cache. Unit is
number of 8 kB blocks. The default is 8192, which means 64 MB.

#### max_file_descriptors

Max number of file descriptors to hold open concurrently for accessing
layer files. This should be kept well below the process/container/OS
limit (see `ulimit -n`), as the pageserver also needs file descriptors
for other files and for sockets for incoming connections.

#### pg_distrib_dir

A directory with Postgres installation to use during pageserver activities.
Since pageserver supports several postgres versions, `pg_distrib_dir` contains
a subdirectory for each version with naming convention `v{PG_MAJOR_VERSION}/`.
Inside that dir, a `bin/postgres` binary should be present.

The default distrib dir is `./pg_install/`.

#### workdir (-D)

A directory in the file system, where pageserver will store its files.
The default is `./.neon/`.

This parameter has a special CLI alias (`-D`) and can not be overridden with regular `-c` way.

##### Remote storage

There's a way to automatically back up and restore some of the pageserver's data from working dir to the remote storage.
The backup system is disabled by default and can be enabled for either of the currently available storages:

###### Local FS storage

Pageserver can back up and restore some of its workdir contents to another directory.
For that, only a path to that directory needs to be specified as a parameter:

```toml
[remote_storage]
local_path = '/some/local/path/'
```

###### S3 storage

Pageserver can back up and restore some of its workdir contents to S3.
Full set of S3 credentials is needed for that as parameters.
Configuration example:

```toml
[remote_storage]
# Name of the bucket to connect to
bucket_name = 'some-sample-bucket'

# Name of the region where the bucket is located at
bucket_region = 'eu-north-1'

# A "subfolder" in the bucket, to use the same bucket separately by multiple pageservers at once.
# Optional, pageserver uses entire bucket if the prefix is not specified.
prefix_in_bucket = '/some/prefix/'

# S3 API query limit to avoid getting errors/throttling from AWS.
concurrency_limit = 100
```

If no IAM bucket access is used during the remote storage usage, use the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables to set the access credentials.

###### General remote storage configuration

Pageserver allows only one remote storage configured concurrently and errors if parameters from multiple different remote configurations are used.
No default values are used for the remote storage configuration parameters.

Besides, there are parameters common for all types of remote storage that can be configured, those have defaults:

```toml
[remote_storage]
# Max number of concurrent timeline synchronized (layers uploaded or downloaded) with the remote storage at the same time.
max_concurrent_syncs = 50

# Max number of errors a single task can have before it's considered failed and not attempted to run anymore.
max_sync_errors = 10
```

## safekeeper

TODO
