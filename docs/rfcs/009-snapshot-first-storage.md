# Snapshot-first storage architecture

Goals:
- Long-term storage of database pages.
- Easy snapshots; simple snapshot and branch management.
- Allow cloud-based snapshot/branch management.
- Allow cloud-centric branching; decouple branch state from running pageserver.
- Allow customer ownership of data via s3 permissions.
- Provide same or better performance for typical workloads, vs plain postgres.

Non-goals:
- Service database reads from s3 (reads should be serviced from the pageserver cache).
- Keep every version of every page / Implement point-in-time recovery (possibly a future paid feature, based on WAL replay from an existing snapshot).

## Principle of operation

The database “lives in s3”. This means that all of the long term page storage is in s3, and the “live database”-- the version that lives in the pageserver-- is a set of “dirty pages” that haven’t yet been written back to s3.

In practice, this is mostly similar to storing frequent snapshots to s3 of a database that lives primarily elsewhere.

The main difference is that s3 is authoritative about which branches exist; pageservers consume branches, snapshots, and related metadata by reading them from s3. This allows cloud-based management of branches and snapshots, regardless of whether a pageserver is running or not.

It’s expected that a pageserver should keep a copy of all pages, to shield users from s3 latency. A cheap/slow pageserver that falls back to s3 for some reads would be possible, but doesn’t seem very useful right now.

Because s3 keeps all history, and the safekeeper(s) preserve any WAL records needed to reconstruct the most recent changes, the pageserver can store dirty pages in RAM or using non-durable local storage; this should allow very good write performance, since there is no need for fsync or journaling.

Objects in s3 are immutable snapshots, never to be modified once written (only deleted).

Objects in s3 are files, each containing a set of pages for some branch/relation/segment as of a specific time (LSN). A snapshot could be complete (meaning it has a copy of every page), or it could be incremental (containing only the pages that were modified since the previous snapshot). It’s expected that most snapshots are incremental to keep storage costs low.

It’s expected that the pageserver would upload new snapshot objects frequently, e.g. somewhere between 30 seconds and 15 minutes, depending on cost/performance balance.

No-longer needed snapshots can be “squashed”-- meaning snapshot N and snapshot N+1 can be read by some cloud agent software, which writes out a new object containing the combined set of pages (keeping only the newest version of each page) and then deletes the original snapshots.

A pageserver only needs to store the set of pages needed to satisfy operations in flight: if a snapshot is still being written, the pageserver needs to hold historical pages so that snapshot captures a consistent moment in time (similar to what is needed to satisfy a slow replica).

WAL records can be discarded once a snapshot has been stored to s3. (Unless we want to keep them longer as part of a point-in-time recovery feature.)

## Pageserver operation

To start a pageserver from a stored snapshot, the pageserver downloads a set of snapshots sufficient to start handling requests. We assume this includes the latest copy of every page, though it might be possible to start handling requests early, and retrieve pages for the first time only when needed.

To halt a pageserver, one final snapshot should be written containing all pending WAL updates; then the pageserver and safekeepers can shut down.

It’s assumed there is some cloud management service that ensures only one pageserver is active and servicing writes to a given branch.

The pageserver needs to be able to track whether a given page has been modified since the last snapshot, and should be able to produce the set of dirty pages efficiently to create a new snapshot.

The pageserver need only store pages that are “reachable” from a particular LSN. For example, a page may be written four times, at LSN 100, 200, 300, and 400. If no snapshot is being created when LSN 200 is written, the page at LSN 100 can be discarded. If a snapshot is triggered when the pageserver is at LSN 299, the pageserver must preserve the page from LSN 200 until that snapshot is complete. As before, the page at LSN 300 can be discarded when the LSN 400 pages is written (regardless of whether the LSN 200 snapshot has completed.)

If the pageserver is servicing multiple branches, those branches may contain common history. While it would be possible to serve branches with zero knowledge of their common history, a pageserver could save a lot of space using an awareness of branch history to share the common set of pages. Computing the “liveness” of a historical page may be tricky in the face of multiple branches.

The pageserver may store dirty pages to memory or to local block storage; any local block storage format is only temporary “overflow” storage, and is not expected to be readable by future software versions.

The pageserver may store clean pages (those that are captured in a snapshot) any way it likes: in memory, in a local filesystem (possibly keeping a local copy of the snapshot file), or using some custom storage format. Reading pages from s3 would be functional, but is expected to be prohibitively slow.

The mechanism for recovery after a pageserver failure is WAL redo. If we find that too slow in some situations (e.g. write-heavy workload causes long startup), we can write more frequent snapshots to keep the number of outstanding WAL records low. If that’s still not good enough, we could look at other options (e.g. redundant pageserver or an EBS page journal).

A read-only pageserver is possible; such a pageserver could be a read-only cache of a specific snapshot, or could auto-update to the latest snapshot on some branch. Either way, no safekeeper is required. Multiple read-only pageservers could exist for a single branch or snapshot.

## Cloud snapshot manager operation

Cloud software may wish to do the following operations (commanded by a user, or based on some pre-programmed policy or other cloud agent):
Create/delete/clone/rename a database
Create a new branch (possibly from a historical snapshot)
Start/stop the pageserver/safekeeper on a branch
List databases/branches/snapshots that are visible to this user account

Some metadata operations (e.g. list branches/snapshots of a particular db) could be performed by scanning the contents of a bucket and inspecting the file headers of each snapshot object. This might not be fast enough; it might be necessary to build a metadata service that can respond more quickly to some queries.

This is especially true if there are public databases: there may be many thousands of buckets that are public, and scanning all of them is not a practical strategy for answering metadata queries.

## Snapshot names, deletion and concurrency

There may be race conditions between operations-- in particular, a “squash” operation may replace two snapshot objects (A, B) with some combined object (C). Since C is logically equivalent to B, anything that attempts to access B should be able to seamlessly switch over to C. It’s assumed that concurrent delete won’t disrupt a read in flight, but it may be possible for some process to read B’s header, and then discover on the next operation that B is gone.

For this reason, any attempted read should attempt a fallback procedure (list objects; search list for an equivalent object) if an attempted read fails.  This requires a predictable naming scheme, e.g. `XXXX_YYYY_ZZZZ_DDDD`, where `XXXX` is the branch unique id, and `YYYY` and `ZZZZ` are the starting/ending LSN values.  `DDDD` is a timestamp indicating when the object was created; this is used to disambiguate a series of empty snapshots, or to help a snapshot policy engine understand which snapshots should be kept or discarded.

## Branching

A user may request a new branch from the cloud user interface. There is a sequence of things that needs to happen:
- If the branch is supposed to be based on the latest contents, the pageserver should perform an immediate snapshot. This is the parent snapshot for the new branch.
- Cloud software should create the new branch, by generating a new (random) unique branch identifier, and creating a placeholder snapshot object.
    - The placeholder object is an empty snapshot containing only metadata (which anchors it to the right parent history) and no pages.
    - The placeholder can be discarded when the first snapshot (containing data) is completed. Discarding is equivalent to squashing, when the snapshot contains no data.
- If the branch needs to be started immediately, a pageserver should be notified that it needs to start servicing the branch. This may not be the same pageserver that services the parent branch, though the common history may make it the best choice.

Some of these steps could be combined into the pageserver, but that process would not be possible under all cases (e.g. if no pageserver is currently running, or if the branch is based on an older snapshot, or if a different pageserver will be serving the new branch). Regardless of which software drives the process, the result should look the same.

## Long-term file format

Snapshot files (and any other object stored in s3) must be readable by future software versions.

It should be possible to build multiple tools (in addition to the pageserver) that can read and write this file format-- for example, to allow cloud snapshot management.

Files should contain the following metadata, in addition to the set of pages:
- The version of the file format.
- A unique identifier for this branch (should be worldwide-unique and unchanging).
- Optionally, any human-readable names assigned to this branch (for management UI/debugging/logging).
- For incremental snapshots, the identifier of the predecessor snapshot. For new branches, this will be the parent snapshot (the point at which history diverges).
- The location of the predecessor branch snapshot, if different from this branch’s location.
- The LSN range `(parent, latest]` for this snapshot. For complete snapshots, the parent LSN can be 0.
- The UTC timestamp of the snapshot creation (which may be different from the time of its highest LSN, if the database is idle).
- A SHA2 checksum over the entire file (excluding the checksum itself), to preserve file integrity.

A file may contain no pages, and an empty LSN range (probably `(latest, latest]`?), which serves as a placeholder for either a newly-created branch, or a snapshot of an idle database.

Any human-readable names stored in the file may fall out of date if database/branch renames are allowed; there may need to be a cloud metadata service to query (current name -> unique identifier). We may choose instead to not store human-readable names in the database, or treat them as debugging information only.

## S3 semantics, and other kinds of storage

For development and testing, it may be easier to use other kinds of storage in place of s3. For example, a directory full of files can substitute for an s3 bucket with multiple objects. This mode is expected to match the s3 semantics (e.g. don’t edit existing files or use symlinks). Unit tests may omit files entirely and use an in-memory mock bucket.

Some users may want to use a local or network filesystem in place of s3. This isn’t prohibited but it’s not a priority, either.

Alternate implementations of s3 should be supported, including Google Cloud Storage.

Azure Blob Storage should be supported. We assume (without evidence) that it’s semantically equivalent to s3 for this purpose.

The properties of s3 that we depend on are:
list objects
streaming read of entire object
read byte range from object
streaming write new object (may use multipart upload for better reliability)
delete object (that should not disrupt an already-started read).

Uploaded files, restored backups, or s3 buckets controlled by users could contain malicious content. We should always validate that objects contain the content they’re supposed to. Incorrect, Corrupt or malicious-looking contents should cause software (cloud tools, pageserver) to fail gracefully.

## Notes

Possible simplifications, for a first draft implementation:
- Assume that dirty pages fit in pageserver RAM. Can use kernel virtual memory to page out to disk if needed. Can improve this later.
- Don’t worry about the details of the squashing process yet.
- Don’t implement cloud metadata service; try to make everything work using basic s3 list-objects and reads.
- Don’t implement rename, delete at first.
- Don’t implement public/private, just use s3 permissions.
- Don’t worry about sharing history yet-- each user has their own bucket and a full copy of all data.
- Don’t worry about history that spans multiple buckets.
- Don’t worry about s3 regions.
- Don’t support user-writeable s3 buckets; users get only read-only access at most.

Open questions:
- How important is point-in-time recovery? When should we add this? How should it work?
- Should snapshot files use compression?
- Should we use snapshots for async replication? A spare pageserver could stay mostly warmed up by consuming snapshots as they’re created.
- Should manual snapshots, or snapshots triggered by branch creation, be named differently from snapshots that are triggered by a snapshot policy?
- When a new branch is created, should it always be served by the same pageserver that owns its parent branch? When should we start a new pageserver?
- How can pageserver software upgrade be done with minimal downtime?
