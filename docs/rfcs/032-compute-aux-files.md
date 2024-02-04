# Postgres aux files storage

In the current Neon architecture compute can only persist data through WAL replication to storage nodes. That covers most of the data that postgres persist, but not all. Exception include:
* replication slots
* postgres statistics files
* pg_stat_statements disk state
* pg_prewarm disk state

It also may be benefitial to store observability data like compute metrics and page access histograms in the per-endpoint storage.

## Aux file types

Mentioned files have different requirements on durability, have different IO patterns, expected size and desired behaviuor with respect to point-in-time recovery and read-only replicas.

### replication slots

Replication slot persists data about replication receiver on replication source. In case of chain replication that files could be modified by replica (because it may have another attached replica). Ability to modify that state on replica is the main reason for this data to stay in a separate file instead of in usual relation (replica can't modify data).

* stale file: not allowed, as postgres might already prunned WAL or vacuumed old catalog entries (relevant in case of logical replication)
* lost file: postgres will start, but replication will not be able to proceed
* pitr: resetting slot to previious point in time is meaningful in a sense that one will be able to continue replication from that point in time. At the same it hard to come up with use case where that would be useful (as replica has to be reset in a similar fashion). Taking into account fact that stale slot will cause WAL and catalog to grow without garbage collection we decided to delete slots on branch creation. That is consistent with postgres documentation suggestion to omit `pg_replslot` from a backup.


There are several types of files:

* Slot state, `pg_replslot/<slot_name>/state` fixed size, about 200b; updated on checkpoint
* LogicalRewriteHeap, `pg_logical/mappings/map-*`, linear with the size of the mapping, not sure how big in practise (should be 100-1000 bytes?) and is there an upper bound; updated on checkpoint
* SnapBuild state -- `pg_logical/snapshots/*`, fixed structure + array of xids, capped on small_const + (4 bytes) * max_transactions * 2, so up to kilobytes of data; updated on checkpoint or running_xact wal record. running_xact is written each 15s so we are ending up with tons of snapshot files.
* ReplicationOrigin -- `pg_logical/replorigin_checkpoint`, fixed size, seems to be capped on 10 bytes; updated on checkpoint

Important point about all that files is that they could be modified on the replica in case of chain replication or logical decoding at standby. Our approach with wal-logging comes at the expense of not being able to support chain replication and logical decoding at standby.

### pgstat

stale file: Looks like it is not okay since postgres discards it on dirty stop
lost file: postgres will work, potential impact on query plans quality
pitr: resetting pgstat makes sense if we can get relevant version of that file. Since dump/restore are only done on stop/start most likely we would need to change postgres code to dump file periodically and be able to deal with outdated file. Dealing with outdated file on start looks more practical then trying to wal-log changes about stats.

Written only on shutdown, read only on startup. Size depend on the number of relations and columns, typically 100Kb - 1Mb range, but it could be more then that. In my tests db it was 1Kb and compressed 10x with gzip.

Stored in `pg_stat/pgstat.stat`

### pg_stat_statements

Stale file isn't supported right now, but should be possible to add if we want to.
Lost file means lost stats, everything else will work.

File is only saved on clean shutdown and read on start. Maximum size depends on `pgss_max` setting (max amount of tracked queries), default value is 5000. I've used `sqlsmith --target=""` to generate random SQL's and fill statement cache to 4967 (can't go higher, some eviction kicks in). With that amount of entries `pg_stat/pg_stat_statements.stat` file was 12MB and compressed to 2.1M with gzip. 

### pg_prewarm

File is only saved on clean shutdown and read on start. No stale files (also possible to add if needed).

Stored in `autoprewarm.blocks` file. Size is `NBuffers * 20 bytes`. NBuffers is amount of 8Kb pages in shared buffers. So for 16GB shared buffers this file would be ~40MB and will grow lineraly with shared buffers size. We should take into account that size of shared buffers might change.


## summary on different files

Files that are associated with repslots are a bit different from the rest in two ways:
    1. Durability requirement is higher. If we lose repslot we can't regenerate it and replication will break
    2. Access happens while postgres is running, not only on start and shutdown

With stats files durability stakes are lower, since we can regenerate this files, but have to pay performance impact for that.

With all of that files we do not need PITR in fact. It would be nice-to-have thing, but not a necessary one.

## Possible storages

### s3fs

We can mount `/<branch_id>/<endpoint_id>` directory into each running compute.

Pros:

* we don't have to instrument postgres code with special file access routines (wal logging on write or some api access on read/write).
* With fs API we have buit-in laziness of access, so don't have to download prewarm/stats/pgss files to start database (respective background workers will read them)

Cons:

* Mount time will contribute to start latency. And some slot files have to be downloaded for postgres to start. Here we should be accurate with batch access to that files, otherwise we can have slow start in case of thouthands of small files. TODO: exact list of files that can block start. Are snapshot files are read on start?

It not super clear how to exactly mount S3 in our setup. We should restrict access to only a certain prefix, e.g. `/<branch_id>/<endpoint_id>`. Then we have to mount it somehow. AWS has `s3-csi`, but that things is mounted on pod start and with our approach of pre-created computes we don't know endpoint/tenant during pod start. So we have to manage mount inside pod. That means setting `cap_admin` on pod. So not sure how be with pod. On the othe hand things are easier with NeonVM: that way inside of each pod we have VM and we have root access to it.

TODO: figure out how to share credentials with vm and how to restrict access to a certain prefix.

NB: s3 fs is not a general purpose FS -- no in-place updates, high latency, etc. So not a good fit for general temp files. But ok for all files mentioned here.

### EBS

EBS is per-AZ, so we will introduce branch/endpoint dependency on AZ, that is not good.

### EFS

Built on top of NFS. We could potentially use it. Same problems with mounting as with `s3fs`. Harder to restrict access to a certain path. We had problems with Vector on EFS that resulted in corrupted files (could be Vector issue).

### Logical messages + basebackup

That is what we have now.

Pros:

* no need in extra service (whether ours or external)

Cons:

* in its current form no laziness on start, we load all files via basebackup
* no file updates on replicas, which is needed for hcain replication and logical decoding on standby

### Centralized service

Pros:
* Flexible

Cons:
* We would have to instrument all accesses. Both reads and writes.
