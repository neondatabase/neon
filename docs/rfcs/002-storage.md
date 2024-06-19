# Neon storage node — alternative

## **Design considerations**

Simplify storage operations for people => Gain adoption/installs on laptops and small private installation => Attract customers to DBaaS by seamless integration between our tooling and cloud.

Proposed architecture addresses:

- High availability -- tolerates n/2 - 1 failures
- Multi-tenancy -- one storage for all databases
- Elasticity -- increase storage size on the go by adding nodes
- Snapshots / backups / PITR with S3 offload
- Compression

Minuses are:

- Quite a lot of work
- Single page access may touch few disk pages
- Some bloat in data — may slowdown sequential scans

## **Summary**

Storage cluster is sharded key-value store with ordered keys. Key (****page_key****) is a tuple of `(pg_id, db_id, timeline_id, rel_id, forkno, segno, pageno, lsn)`. Value is either page or page diff/wal. Each chunk (chunk == shard) stores approx 50-100GB ~~and automatically splits in half when grows bigger then soft 100GB limit~~. by having a fixed range of pageno's it is responsible for. Chunks placement on storage nodes is stored in a separate metadata service, so chunk can be freely moved around the cluster if it is need. Chunk itself is a filesystem directory with following sub directories:

```

|-chunk_42/
  |-store/ -- contains lsm with pages/pagediffs ranging from
  |	      page_key_lo to page_key_hi
  |-wal/
  |  |- db_1234/ db-specific wal files with pages from page_key_lo
  |		 to page_key_hi
  |
  |-chunk.meta -- small file with snapshot references
		  (page_key_prefix+lsn+name)
		  and PITR regions (page_key_start, page_key_end)
```

## **Chunk**

Chunk is responsible for storing pages potentially from different databases and relations. Each page is addressed by a lexicographically ordered tuple (****page_key****) with following fields:

- `pg_id` -- unique id of given postgres instance (or postgres cluster as it is called in postgres docs)
- `db_id` -- database that was created by 'CREATE DATABASE' in a given postgres instance
- `db_timeline` -- used to create Copy-on-Write instances from snapshots, described later
- `rel_id` -- tuple of (relation_id, 0) for tables and (indexed_relation_id, rel_id) for indices. Done this way so table indices were closer to table itself on our global key space.
- `(forkno, segno, pageno)` -- page coordinates in postgres data files
- `lsn_timeline` -- postgres feature, increments when PITR was done.
- `lsn` -- lsn of current page version.

Chunk stores pages and page diffs ranging from page_key_lo to page_key_hi. Processing node looks at page in wal record and sends record to a chunk responsible for this page range. When wal record arrives to a chunk it is initially stored in `chunk_id/wal/db_id/wal_segno.wal`. Then background process moves records from that wal files to the lsm tree in `chunk_id/store`. Or, more precisely, wal records would be materialized into lsm memtable and when that memtable is flushed to SSTable on disk we may trim the wal. That way some not durably (in the distributed sense) committed pages may enter the tree -- here we rely on processing node behavior: page request from processing node should contain proper lsm horizons so that storage node may respond with proper page version.

LSM here is a usual LSM for variable-length values: at first data is stored in memory (we hold incoming wal records to be able to regenerate it after restart) at some balanced tree. When this tree grows big enough we dump it into disk file (SSTable) sorting records by key. Then SStables are mergesorted in the background to a different files. All file operation are sequential and do not require WAL for durability.

Content of SSTable can be following:

```jsx
(pg_id, db_id, ... , pageno=42, lsn=100) (full 8k page data)
(pg_id, db_id, ... , pageno=42, lsn=150) (per-page diff)
(pg_id, db_id, ... , pageno=42, lsn=180) (per-page diff)
(pg_id, db_id, ... , pageno=42, lsn=200) (per-page diff)
(pg_id, db_id, ... , pageno=42, lsn=220) (full 8k page data)
(pg_id, db_id, ... , pageno=42, lsn=250) (per-page diff)
(pg_id, db_id, ... , pageno=42, lsn=270) (per-page diff)
(pg_id, db_id, ... , pageno=5000, lsn=100) (full 8k page data)
```

So query for `pageno=42 up to lsn=260` would need to find closest entry less then this key, iterate back to the latest full page and iterate forward to apply diffs. How often page is materialized in lsn-version sequence is up to us -- let's say each 5th version should be a full page.

### **Page deletion**

To delete old pages we insert blind deletion marker `(pg_id, db_id, #trim_lsn < 150)` into a lsm tree. During merges such marker would indicate that all pages with smaller lsn should be discarded. Delete marker will travel down the tree levels hierarchy until it reaches last level. In non-PITR scenario where old page version are not needed at all such deletion marker would (in average) prevent old page versions propagation down the tree -- so all bloat would concentrate at higher tree layers without affecting bigger bottom layers.

### **Recovery**

Upon storage node restart recent WAL files are applied to appropriate pages and resulting pages stored in lsm memtable. So this should be fast since we are not writing anything to disk.

### **Checkpointing**

No such mechanism is needed. Or we may look at the storage node as at kind of continuous checkpointer.

### **Full page writes (torn page protection)**

Storage node never updates individual pages, only merges SSTable, so torn pages is not an issue.

### **Snapshot**

That is the part that I like about this design -- snapshot creation is instant and cheap operation that can have flexible granularity level: whole instance, database, table. Snapshot creation inserts a record in `chunk.meta` file with lsn of this snapshot and key prefix `(pg_id, db_id, db_timeline, rel_id, *)` that prohibits pages deletion within this range. Storage node may not know anything about page internals, but by changing number of fields in our prefix we may change snapshot granularity.

It is again useful to remap `rel_id` to `(indexed_relation_id, rel_id)` so that snapshot of relation would include it's indices. Also table snapshot would trickily interact with catalog. Probably all table snapshots should hold also a catalog snapshot. And when node is started with such snapshot it should check that only tables from snapshot are queried. I assume here that for snapshot reading one need to start a new postgres instance.

Storage consumed by snapshot is proportional to the amount of data changed. We may have some heuristic (calculated based on cost of different storages) about when to offload old snapshot to s3. For example, if current database has more then 40% of changed pages with respect to previous snapshot then we may offload that snapshot to s3, and release this space.

**Starting db from snapshot**

When we are starting database from snapshot it can be done in two ways. First, we may create new db_id, move all the data from snapshot to a new db and start a database. Second option is to create Copy-on-Write (CoW) instance out of snapshot and read old pages from old snapshot and store new pages separately. That is why there is `db_timeline` key field near `db_id` -- CoW (🐮) database should create new `db_timeline` and remember old `db_timeline`. Such a database can have hashmap of pages that it is changed to query pages from proper snapshot on the first try. `db_timeline` is located near `db_id` so that new page versions generated by new instance would not bloat data of initial snapshot. It is not clear for whether it is possibly to effectively support "stacked" CoW snapshot, so we may disallow them. (Well, one way to support them is to move `db_timeline` close to `lsn` -- so we may scan neighboring pages and find right one. But again that way we bloat snapshot with unrelated data and may slowdown full scans that are happening in different database).

**Snapshot export/import**

Once we may start CoW instances it is easy to run auxiliary postgres instance on this snapshot and run `COPY FROM (...) TO stdout` or `pg_dump` and export data from the snapshot to some portable formats. Also we may start postgres on a new empty database and run `COPY FROM stdin`. This way we can initialize new non-CoW databases and transfer snapshots via network.

### **PITR area**

In described scheme PITR is just a prohibition to delete any versions within some key prefix, either it is a database or a table key prefix. So PITR may have different settings for different tables, databases, etc.

PITR is quite bloaty, so we may aggressively offload it to s3 -- we may push same (or bigger) SSTables to s3 and maintain lsm structure there.

### **Compression**

Since we are storing page diffs of variable sizes there is no structural dependency on a page size and we may compress it. Again that could be enabled only on pages with some key prefixes, so we may have this with db/table granularity.

### **Chunk metadata**

Chunk metadata is a file lies in chunk directory that stores info about current snapshots and PITR regions. Chunk should always consult this data when merging SSTables and applying delete markers.

### **Chunk splitting**

*(NB: following paragraph is about how to avoid page splitting)*

When chunks hits some soft storage limit (let's say 100Gb) it should be split in half and global metadata about chunk boundaries should be updated. Here i assume that chunk split is a local operation happening on single node. Process of chink splitting should look like following:

1. Find separation key and spawn two new chunks with [lo, mid) [mid, hi) boundaries.

2. Prohibit WAL deletion and old SSTables deletion on original chunk.

3. On each lsm layer we would need to split only one SSTable, all other would fit within left or right range. Symlink/split that files to new chunks.

4. Start WAL replay on new chunks.

5. Update global metadata about new chunk boundaries.

6. Eventually (metadata update should be pushed to processing node by metadata service) storage node will start sending WAL and page requests to the new nodes.

7. New chunk may start serving read queries when following conditions are met:

a) it receives at least on WAL record from processing node

b) it replayed all WAL up to the new received one

c) checked by downlinks that there were no WAL gaps.

Chunk split as it is described here is quite fast operation when it is happening on the local disk -- vast majority of files will be just moved without copying anything. I suggest to keep split always local and not to mix it with chunk moving around cluster. So if we want to split some chunk but there is small amount of free space left on the device, we should first move some chunks away from the node and then proceed with splitting.

### Fixed chunks

Alternative strategy is to not to split at all and have pageno-fixed chunk boundaries. When table is created we first materialize this chunk by storing first new pages only and chunks is small. Then chunk is growing while table is filled, but it can't grow substantially bigger then allowed pageno range, so at max it would be 1GB or whatever limit we want + some bloat due to snapshots and old page versions.

### **Chunk lsm internals**

So how to implement chunk's lsm?

- Write from scratch and use RocksDB to prototype/benchmark, then switch to own lsm implementation. RocksDB can provide some sanity check for performance of home-brewed implementation and it would be easier to prototype.
- Use postgres as lego constructor. We may model memtable with postgres B-tree referencing some in-memory log of incoming records. SSTable merging may reuse postgres external merging algorithm, etc. One thing that would definitely not fit (or I didn't came up with idea how to fit that) -- is multi-tenancy. If we are storing pages from different databases we can't use postgres buffer pool, since there is no db_id in the page header. We can add new field there but IMO it would be no go for committing that to vanilla.

Other possibility is to not to try to fit few databases in one storage node. But that way it is no go for multi-tenant cloud installation: we would need to run a lot of storage node instances on one physical storage node, all with it own local page cache. So that would be much closer to ordinary managed RDS.

Multi-tenant storage makes sense even on a laptop, when you work with different databases, running tests with temp database, etc. And when installation grows bigger it start to make more and more sense, so it seems important.

# Storage fleet

# **Storage fleet**

- When database is smaller then a chunk size we naturally can store them in one chunk (since their page_key would fit in some chunk's [hi, lo) range).

<img width="937" alt="Screenshot_2021-02-22_at_16 49 17" src="https://user-images.githubusercontent.com/284219/108729836-ffcbd200-753b-11eb-9412-db802ec30021.png">

Few databases are stored in one chunk, replicated three times

- When database can't fit into one storage node it can occupy lots of chunks that were split while database was growing. Chunk placement on nodes is controlled by us with some automatization, but we always may manually move chunks around the cluster.

<img width="940" alt="Screenshot_2021-02-22_at_16 49 10" src="https://user-images.githubusercontent.com/284219/108729815-fb071e00-753b-11eb-86e0-be6703e47d82.png">

Here one big database occupies two set of nodes. Also some chunks were moved around to restore replication factor after disk failure. In this case we also have "sharded" storage for a big database and issue wal writes to different chunks in parallel.

## **Chunk placement strategies**

There are few scenarios where we may want to move chunks around the cluster:

- disk usage on some node is big
- some disk experienced a failure
- some node experienced a failure or need maintenance

## **Chunk replication**

Chunk replication may be done by cloning page ranges with respect to some lsn from peer nodes, updating global metadata, waiting for WAL to come, replaying previous WAL and becoming online -- more or less like during chunk split.

