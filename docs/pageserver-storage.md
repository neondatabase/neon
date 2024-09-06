# Pageserver storage

The main responsibility of the Page Server is to process the incoming WAL, and
reprocess it into a format that allows reasonably quick access to any page
version. The page server slices the incoming WAL per relation and page, and
packages the sliced WAL into suitably-sized "layer files". The layer files
contain all the history of the database, back to some reasonable retention
period. This system replaces the base backups and the WAL archive used in a
traditional PostgreSQL installation. The layer files are immutable, they are not
modified in-place after creation. New layer files are created for new incoming
WAL, and old layer files are removed when they are no longer needed.

The on-disk format is based on immutable files. The page server receives a
stream of incoming WAL, parses the WAL records to determine which pages they
apply to, and accumulates the incoming changes in memory. Whenever enough WAL
has been accumulated in memory, it is written out to a new immutable file. That
process accumulates "L0 delta files" on disk. When enough L0 files have been
accumulated, they are merged and re-partitioned into L1 files, and old files
that are no longer needed are removed by Garbage Collection (GC).

The incoming WAL contains updates to arbitrary pages in the system. The
distribution depends on the workload: the updates could be totally random, or
there could be a long stream of updates to a single relation when data is bulk
loaded, for example, or something in between.

```
Cloud Storage                   Page Server                           Safekeeper
                        L1               L0             Memory            WAL

+----+               +----+----+
|AAAA|               |AAAA|AAAA|      +---+-----+         |
+----+               +----+----+      |   |     |         |AA
|BBBB|               |BBBB|BBBB|      |BB | AA  |         |BB
+----+----+          +----+----+      |C  | BB  |         |CC
|CCCC|CCCC|  <----   |CCCC|CCCC| <--- |D  | CC  |  <---   |DDD     <----   ADEBAABED
+----+----+          +----+----+      |   | DDD |         |E
|DDDD|DDDD|          |DDDD|DDDD|      |E  |     |         |
+----+----+          +----+----+      |   |     |
|EEEE|               |EEEE|EEEE|      +---+-----+
+----+               +----+----+
```

In this illustration, WAL is received as a stream from the Safekeeper, from the
right.  It is immediately captured by the page server and stored quickly in
memory. The page server memory can be thought of as a quick "reorder buffer",
used to hold the incoming WAL and reorder it so that we keep the WAL records for
the same page and relation close to each other.

From the page server memory, whenever enough WAL has been accumulated, it is flushed
to disk into a new L0 layer file, and the memory is released.

When enough L0 files have been accumulated, they are merged together and sliced
per key-space, producing a new set of files where each file contains a more
narrow key range, but larger LSN range.

From the local disk, the layers are further copied to Cloud Storage, for
long-term archival. After a layer has been copied to Cloud Storage, it can be
removed from local disk, although we currently keep everything locally for fast
access. If a layer is needed that isn't found locally, it is fetched from Cloud
Storage and stored in local disk. L0 and L1 files are both uploaded to Cloud
Storage.

# Layer map

The LayerMap tracks what layers exist in a timeline.

Currently, the layer map is just a resizable array (Vec). On a GetPage@LSN or
other read request, the layer map scans through the array to find the right layer
that contains the data for the requested page. The read-code in LayeredTimeline
is aware of the ancestor, and returns data from the ancestor timeline if it's
not found on the current timeline.

# Different kinds of layers

A layer can be in different states:

- Open - a layer where new WAL records can be appended to.
- Closed - a layer that is read-only, no new WAL records can be appended to it
- Historic: synonym for closed
- InMemory: A layer that needs to be rebuilt from WAL on pageserver start.
To avoid OOM errors, InMemory layers can be spilled to disk into ephemeral file.
- OnDisk: A layer that is stored on disk. If its end-LSN is older than
  disk_consistent_lsn, it is known to be fully flushed and fsync'd to local disk.
- Frozen layer: an in-memory layer that is Closed.

TODO: Clarify the difference between Closed, Historic and Frozen.

There are two kinds of OnDisk layers:
- ImageLayer represents a snapshot of all the keys in a particular range, at one
  particular LSN. Any keys that are not present in the ImageLayer are known not
  to exist at that LSN.
- DeltaLayer represents a collection of WAL records or page images in a range of
  LSNs, for a range of keys.

# Layer life cycle

LSN range defined by start_lsn and end_lsn:
- start_lsn is inclusive.
- end_lsn is exclusive.

For an open in-memory layer, the end_lsn is MAX_LSN. For a frozen in-memory
layer or a delta layer, it is a valid end bound. An image layer represents
snapshot at one LSN, so end_lsn is always the snapshot LSN + 1

Every layer starts its life as an Open In-Memory layer. When the page server
receives the first WAL record for a timeline, it creates a new In-Memory layer
for it, and puts it to the layer map. Later, when the layer becomes full, its
contents are written to disk, as an on-disk layers.

Flushing a layer is a two-step process: First, the layer is marked as closed, so
that it no longer accepts new WAL records, and a new in-memory layer is created
to hold any WAL after that point. After this first step, the layer is a Closed
InMemory state. This first step is called "freezing" the layer.

In the second step, a new Delta layers is created, containing all the data from
the Frozen InMemory layer. When it has been created and flushed to disk, the
original frozen layer is replaced with the new layers in the layer map, and the
original frozen layer is dropped, releasing the memory.

# Layer files (On-disk layers)

The files are called "layer files". Each layer file covers a range of keys, and
a range of LSNs (or a single LSN, in case of image layers). You can think of it
as a rectangle in the two-dimensional key-LSN space. The layer files for each
timeline are stored in the timeline's subdirectory under
`.neon/tenants/<tenant_id>/timelines`.

There are two kind of layer files: images, and delta layers. An image file
contains a snapshot of all keys at a particular LSN, whereas a delta file
contains modifications to a segment - mostly in the form of WAL records - in a
range of LSN.

image file:

```
    000000067F000032BE0000400000000070B6-000000067F000032BE0000400000000080B6__00000000346BC568
              start key                          end key                           LSN
```


The first parts define the key range that the layer covers. See
pgdatadir_mapping.rs for how the key space is used. The last part is the LSN.

delta file:

Delta files are named similarly, but they cover a range of LSNs:

```
    000000067F000032BE0000400000000020B6-000000067F000032BE0000400000000030B6__000000578C6B29-0000000057A50051
              start key                          end key                          start LSN     end LSN
```

A delta file contains all the key-values in the key-range that were updated in
the LSN range. If a key has not been modified, there is no trace of it in the
delta layer.


A delta layer file can cover a part of the overall key space, as in the previous
example, or the whole key range like this:

```
    000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000578C6B29-0000000057A50051
```

A file that covers the whole key range is called a L0 file (Level 0), while a
file that covers only part of the key range is called a L1 file. The "level" of
a file is not explicitly stored anywhere, you can only distinguish them by
looking at the key range that a file covers. The read-path doesn't need to
treat L0 and L1 files any differently.


## Notation used in this document

FIXME: This is somewhat obsolete, the layer files cover a key-range rather than
a particular relation nowadays. However, the description on how you find a page
version, and how branching and GC works is still valid.

The full path of a delta file looks like this:

```
    .neon/tenants/941ddc8604413b88b3d208bddf90396c/timelines/4af489b06af8eed9e27a841775616962/rel_1663_13990_2609_0_10_000000000169C348_0000000001702000
```

For simplicity, the examples below use a simplified notation for the
paths.  The tenant ID is left out, the timeline ID is replaced with
the human-readable branch name, and spcnode+dbnode+relnode+forkum+segno
with a human-readable table name. The LSNs are also shorter. For
example, a base image file at LSN 100 and a delta file between 100-200
for 'orders' table on 'main' branch is represented like this:

```
    main/orders_100
    main/orders_100_200
```


# Creating layer files

Let's start with a simple example with a system that contains one
branch called 'main' and two tables, 'orders' and 'customers'. The end
of WAL is currently at LSN 250. In this starting situation, you would
have these files on disk:

```
	main/orders_100
	main/orders_100_200
	main/orders_200
	main/customers_100
	main/customers_100_200
	main/customers_200
```

In addition to those files, the recent changes between LSN 200 and the
end of WAL at 250 are kept in memory. If the page server crashes, the
latest records between 200-250 need to be re-read from the WAL.

Whenever enough WAL has been accumulated in memory, the page server
writes out the changes in memory into new layer files. This process
is called "checkpointing" (not to be confused with the PostgreSQL
checkpoints, that's a different thing). The page server only creates
layer files for relations that have been modified since the last
checkpoint. For example, if the current end of WAL is at LSN 450, and
the last checkpoint happened at LSN 400 but there hasn't been any
recent changes to 'customers' table, you would have these files on
disk:

	main/orders_100
	main/orders_100_200
	main/orders_200
	main/orders_200_300
	main/orders_300
	main/orders_300_400
	main/orders_400
	main/customers_100
	main/customers_100_200
	main/customers_200

If the customers table is modified later, a new file is created for it
at the next checkpoint. The new file will cover the "gap" from the
last layer file, so the LSN ranges are always contiguous:

```
	main/orders_100
	main/orders_100_200
	main/orders_200
	main/orders_200_300
	main/orders_300
	main/orders_300_400
	main/orders_400
	main/customers_100
	main/customers_100_200
	main/customers_200
	main/customers_200_500
	main/customers_500
```

## Reading page versions

Whenever a GetPage@LSN request comes in from the compute node, the
page server needs to reconstruct the requested page, as it was at the
requested LSN. To do that, the page server first checks the recent
in-memory layer; if the requested page version is found there, it can
be returned immediately without looking at the files on
disk. Otherwise the page server needs to locate the layer file that
contains the requested page version.

For example, if a request comes in for table 'orders' at LSN 250, the
page server would load the 'main/orders_200_300' file into memory, and
reconstruct and return the requested page from it, as it was at
LSN 250. Because the layer file consists of a full image of the
relation at the start LSN and the WAL, reconstructing the page
involves replaying any WAL records applicable to the page between LSNs
200-250, starting from the base image at LSN 200.

# Multiple branches

Imagine that a child branch is created at LSN 250:

```
            @250
    ----main--+-------------------------->
               \
                +---child-------------->
```


Then, the 'orders' table is updated differently on the 'main' and
'child' branches. You now have this situation on disk:

```
    main/orders_100
    main/orders_100_200
    main/orders_200
    main/orders_200_300
    main/orders_300
    main/orders_300_400
    main/orders_400
    main/customers_100
    main/customers_100_200
    main/customers_200
    child/orders_250_300
    child/orders_300
    child/orders_300_400
    child/orders_400
```

Because the 'customers' table hasn't been modified on the child
branch, there is no file for it there. If you request a page for it on
the 'child' branch, the page server will not find any layer file
for it in the 'child' directory, so it will recurse to look into the
parent 'main' branch instead.

From the 'child' branch's point of view, the history for each relation
is linear, and the request's LSN identifies unambiguously which file
you need to look at. For example, the history for the 'orders' table
on the 'main' branch consists of these files:

```
    main/orders_100
    main/orders_100_200
    main/orders_200
    main/orders_200_300
    main/orders_300
    main/orders_300_400
    main/orders_400
```

And from the 'child' branch's point of view, it consists of these
files:

```
    main/orders_100
    main/orders_100_200
    main/orders_200
    main/orders_200_300
    child/orders_250_300
    child/orders_300
    child/orders_300_400
    child/orders_400
```

The branch metadata includes the point where the child branch was
created, LSN 250. If a page request comes with LSN 275, we read the
page version from the 'child/orders_250_300' file. We might also
need to reconstruct the page version as it was at LSN 250, in order
to replay the WAL up to LSN 275, using 'main/orders_200_300' and
'main/orders_200'. The page versions between 250-300 in the
'main/orders_200_300' file are ignored when operating on the child
branch.

Note: It doesn't make any difference if the child branch is created
when the end of the main branch was at LSN 250, or later when the tip of
the main branch had already moved on. The latter case, creating a
branch at a historic LSN, is how we support PITR in Neon.


# Garbage collection

In this scheme, we keep creating new layer files over time. We also
need a mechanism to remove old files that are no longer needed,
because disk space isn't infinite.

What files are still needed? Currently, the page server supports PITR
and branching from any branch at any LSN that is "recent enough" from
the tip of the branch.  "Recent enough" is defined as an LSN horizon,
which by default is 64 MB.  (See DEFAULT_GC_HORIZON). For this
example, let's assume that the LSN horizon is 150 units.

Let's look at the single branch scenario again. Imagine that the end
of the branch is LSN 525, so that the GC horizon is currently at
525-150 = 375

```
	main/orders_100
	main/orders_100_200
	main/orders_200
	main/orders_200_300
	main/orders_300
	main/orders_300_400
	main/orders_400
	main/orders_400_500
	main/orders_500
	main/customers_100
	main/customers_100_200
	main/customers_200
```

We can remove the following files because the end LSNs of those files are
older than GC horizon 375, and there are more recent layer files for the
table:

```
	main/orders_100       DELETE
	main/orders_100_200   DELETE
	main/orders_200       DELETE
	main/orders_200_300   DELETE
	main/orders_300       STILL NEEDED BY orders_300_400
	main/orders_300_400   KEEP, NEWER THAN GC HORIZON
	main/orders_400       ..
	main/orders_400_500   ..
	main/orders_500       ..
	main/customers_100      DELETE
	main/customers_100_200  DELETE
	main/customers_200      KEEP, NO NEWER VERSION
```

'main/customers_200' is old enough, but it cannot be
removed because there is no newer layer file for the table.

Things get slightly more complicated with multiple branches. All of
the above still holds, but in addition to recent files we must also
retain older snapshot files that are still needed by child branches.
For example, if child branch is created at LSN 150, and the 'customers'
table is updated on the branch, you would have these files:

```
	main/orders_100        KEEP, NEEDED BY child BRANCH
	main/orders_100_200    KEEP, NEEDED BY child BRANCH
	main/orders_200        DELETE
	main/orders_200_300    DELETE
	main/orders_300        KEEP, NEWER THAN GC HORIZON
	main/orders_300_400    KEEP, NEWER THAN GC HORIZON
	main/orders_400        KEEP, NEWER THAN GC HORIZON
	main/orders_400_500    KEEP, NEWER THAN GC HORIZON
	main/orders_500        KEEP, NEWER THAN GC HORIZON
	main/customers_100       DELETE
	main/customers_100_200   DELETE
	main/customers_200       KEEP, NO NEWER VERSION
	child/customers_150_300  DELETE
	child/customers_300      KEEP, NO NEWER VERSION
```

In this situation, 'main/orders_100' and 'main/orders_100_200' cannot
be removed, even though they are older than the GC horizon, because
they are still needed by the child branch. 'main/orders_200'
and 'main/orders_200_300' can still be removed.

If 'orders' is modified later on the 'child' branch, we will create a
new base image and delta file for it on the child:

```
	main/orders_100
	main/orders_100_200

	main/orders_300
	main/orders_300_400
	main/orders_400
	main/orders_400_500
	main/orders_500
	main/customers_200
	child/customers_300
	child/orders_150_400
	child/orders_400
```

After this, the 'main/orders_100' and 'main/orders_100_200' file could
be removed. It is no longer needed by the child branch, because there
is a newer layer file there. TODO: This optimization hasn't been
implemented! The GC algorithm will currently keep the file on the
'main' branch anyway, for as long as the child branch exists.

TODO:
Describe GC and checkpoint interval settings.

# TODO: On LSN ranges

In principle, each relation can be checkpointed separately, i.e. the
LSN ranges of the files don't need to line up. So this would be legal:

```
	main/orders_100
	main/orders_100_200
	main/orders_200
	main/orders_200_300
	main/orders_300
	main/orders_300_400
	main/orders_400
	main/customers_150
	main/customers_150_250
	main/customers_250
	main/customers_250_500
	main/customers_500
```

However, the code currently always checkpoints all relations together.
So that situation doesn't arise in practice.

It would also be OK to have overlapping LSN ranges for the same relation:

	main/orders_100
	main/orders_100_200
	main/orders_200
	main/orders_200_300
	main/orders_300
	main/orders_250_350
	main/orders_350
	main/orders_300_400
	main/orders_400

The code that reads the layer files should cope with this, but this
situation doesn't arise either, because the checkpointing code never
does that.  It could be useful, however, as a transient state when
garbage collecting around branch points, or explicit recovery
points. For example, if we start with this:

```
	main/orders_100
	main/orders_100_200
	main/orders_200
	main/orders_200_300
	main/orders_300
```

And there is a branch or explicit recovery point at LSN 150, we could
replace 'main/orders_100_200' with 'main/orders_150' to keep a
layer only at that exact point that's still needed, removing the
other page versions around it. But such compaction has not been
implemented yet.
