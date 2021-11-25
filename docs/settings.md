## Pageserver

### listen_pg_addr

Network interface and port number to listen at for connections from
the compute nodes and safekeepers. The default is `127.0.0.1:64000`.

### listen_http_addr

Network interface and port number to listen at for admin connections.
The default is `127.0.0.1:9898`.

### checkpoint_distance

`checkpoint_distance` is the amount of incoming WAL that is held in
the open layer, before it's flushed to local disk. It puts an upper
bound on how much WAL needs to be re-processed after a pageserver
crash. It is a soft limit, the pageserver can momentarily go above it,
but it will trigger a checkpoint operation to get it back below the
limit.

`checkpoint_distance` also determines how much WAL needs to be kept
durable in the safekeeper.  The safekeeper must have capacity to hold
this much WAL, with some headroom, otherwise you can get stuck in a
situation where the safekeeper is full and stops accepting new WAL,
but the pageserver is not flushing out and releasing the space in the
safekeeper because it hasn't reached checkpoint_distance yet.

`checkpoint_distance` also controls how often the WAL is uploaded to
S3.

The unit is # of bytes.

### checkpoint_period

The pageserver checks whether `checkpoint_distance` has been reached
every `checkpoint_period` seconds. Default is 1 s, which should be
fine.

### gc_horizon

`gz_horizon` determines how much history is retained, to allow
branching and read replicas at an older point in time. The unit is #
of bytes of WAL. Page versions older than this are garbage collected
away.

### gc_period

Interval at which garbage collection is triggered. Default is 100 s.

### superuser

Name of the initial superuser role, passed to initdb when a new tenant
is initialized. It doesn't affect anything after initialization. The
default is Note: The default is 'zenith_admin', and the console
depends on that, so if you change it, bad things will happen.

### page_cache_size

Size of the page cache, to hold materialized page versions. Unit is
number of 8 kB blocks. The default is 8192, which means 64 MB.

### max_file_descriptors

Max number of file descriptors to hold open concurrently for accessing
layer files. This should be kept well below the process/container/OS
limit (see `ulimit -n`), as the pageserver also needs file descriptors
for other files and for sockets for incoming connections.

### postgres-distrib

A directory with Postgres installation to use during pageserver activities.
Inside that dir, a `bin/postgres` binary should be present.

The default distrib dir is `./tmp_install/`.

### workdir (-D)

A directory in the file system, where pageserver will store its files.
The default is `./.zenith/`.

### Remote storage

There's a way to automatically backup and restore some of the pageserver's data from working dir to the remote storage.
The backup system is disabled by default and can be enabled for either of the currently available storages:

#### Local FS storage

##### remote-storage-local-path

Pageserver can back up and restore some of its workdir contents to another directory.
For that, only a path to that directory needs to be specified as a parameter.

#### S3 storage

Pageserver can back up and restore some of its workdir contents to S3.
Full set of S3 credentials is needed for that as parameters:

##### remote-storage-s3-bucket

Name of the bucket to connect to, example: "some-sample-bucket".

##### remote-storage-region

Name of the region where the bucket is located at, example: "eu-north-1"

##### remote-storage-access-key

Access key to connect to the bucket ("login" part of the credentials), example: "AKIAIOSFODNN7EXAMPLE"

##### remote-storage-secret-access-key

Secret access key to connect to the bucket ("password" part of the credentials), example: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

#### General remote storage configuration

Pagesever allows only one remote storage configured concurrently and errors if parameters from multiple different remote configurations are used.
No default values are used for the remote storage configuration parameters.

##### remote-storage-max-concurrent-sync

Max number of concurrent connections to open for uploading to or
downloading from S3.
The default value is 100.

## safekeeper

TODO
