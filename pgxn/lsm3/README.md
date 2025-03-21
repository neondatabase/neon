LSM tree implemented using standard Postgres B-Tree indexes.
Top index is used to perform fast inserts and on overflow it is merged
with base index. To perform merge operation concurrently
without blocking other operations with index, two top indexes are used:
active and merged. So totally there are three B-Tree indexes:
two top indexes and one base index.
When performing index scan we have to merge scans of all this three indexes.

This extension needs to create data structure in shared memory and this is why it should be loaded through
"shared_preload_library" list. Once extension is created, you can define indexes using lsm3 access method:

```sql
create extension lsm3;
create table t(id integer, val text);
create index idx on t using lsm3(id);
```

`Lsm3` provides for the same types and set of operations as standard B-Tree.

Current restrictions of `Lsm3`:
- Parallel index scan is not supported.
- Array keys are not supported.
- `Lsm3` index can not be declared as unique.

`Lsm3` extension can be configured using the following parameters:
- `lsm3.max_indexes`: maximal number of Lsm3 indexes (default 1024).
- `lsm3.top_index_size`: size (kb) of top index (default 64Mb).

It is also possible to specify size of top index in relation options - this value will override `lsm3.top_index_size` GUC.

Although unique constraint can not be enforced using Lsm3 index, it is still possible to mark index as unique to
optimize index search. If index is marked as unique and searched key is found in active
top index, then lookup in other two indexes is not performed. As far as application is most frequently
searching for last recently inserted data, we can speedup this search by performing just one index lookup instead of 3.
Index can be marked as unique using index options:

```sql
create index idx on t using lsm3(id) with (unique=true);
```

Please notice that Lsm3 creates bgworker merge process for each Lsm3 index.
So you may need to adjust `max_worker_processes` in postgresql.conf to be large enough.
