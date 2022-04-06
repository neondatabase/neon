# Repository format

A Zenith repository is similar to a traditional PostgreSQL backup
archive, like a WAL-G bucket or pgbarman backup catalogue. It holds
multiple versions of a PostgreSQL database cluster.

The distinguishing feature is that you can launch a Zenith Postgres
server directly against a branch in the repository, without having to
"restore" it first. Also, Zenith manages the storage automatically,
there is no separation between full and incremental backups nor WAL
archive. Zenith relies heavily on the WAL, and uses concepts similar
to incremental backups and WAL archiving internally, but it is hidden
from the user.

## Directory structure, version 1

This first version is pretty straightforward but not very
efficient. Just something to get us started.

The repository directory looks like this:

    .zenith/timelines/4543be3daeab2ed4e58a285cbb8dd1fce6970f8c/wal/
    .zenith/timelines/4543be3daeab2ed4e58a285cbb8dd1fce6970f8c/snapshots/<lsn>/
    .zenith/timelines/4543be3daeab2ed4e58a285cbb8dd1fce6970f8c/history
    
    .zenith/refs/branches/mybranch
    .zenith/refs/tags/foo
    .zenith/refs/tags/bar
    
    .zenith/datadirs/<timeline uuid>

### Timelines

A timeline is similar to PostgeSQL's timeline, but is identified by a
UUID instead of a 32-bit timeline Id.  For user convenience, it can be
given a name that refers to the UUID (called a branch).

All WAL is generated on a timeline. You can launch a read-only node
against a tag or arbitrary LSN on a timeline, but in order to write,
you need to create a timeline.

Each timeline is stored in a directory under .zenith/timelines. It
consists of a WAL archive, containing all the WAL in the standard
PostgreSQL format, under the wal/ subdirectory.

The 'snapshots/' subdirectory, contains "base backups" of the data
directory at a different LSNs. Each snapshot is simply a copy of the
Postgres data directory.

When a new timeline is forked from a previous timeline, the ancestor
timeline's UUID is stored in the 'history' file.

### Refs

There are two kinds of named objects in the repository: branches and
tags.  A branch is a human-friendly name for a timeline UUID, and a
tag is a human-friendly name for a specific LSN on a timeline
(timeline UUID + LSN).  Like in git, these are just for user
convenience; you can also use timeline UUIDs and LSNs directly.

Refs do have one additional purpose though: naming a timeline or LSN
prevents it from being automatically garbage collected.

The refs directory contains a small text file for each tag/branch. It
contains the UUID of the timeline (and LSN, for tags).

### Datadirs

.zenith/datadirs contains PostgreSQL data directories. You can launch
a Postgres instance on one of them with:

```
  postgres -D .zenith/datadirs/4543be3daeab2ed4e58a285cbb8dd1fce6970f8c
```

All the actual data is kept in the timeline directories, under
.zenith/timelines. The data directories are only needed for active
PostgreQSL instances. After an instance is stopped, the data directory
can be safely removed. "zenith start" will recreate it quickly from
the data in .zenith/timelines, if it's missing.

## Version 2

The format described above isn't very different from a traditional
daily base backup + WAL archive configuration. The main difference is
the nicer naming of branches and tags.

That's not very efficient. For performance, we need something like
incremental backups that don't require making a full copy of all
data. So only store modified files or pages. And instead of having to
replay all WAL from the last snapshot, "slice" the WAL into
per-relation WAL files and only recover what's needed when a table is
accessed.

In version 2, the file format in the "snapshots" subdirectory gets
more advanced. The exact format is TODO. But it should support:
- storing WAL records of individual relations/pages
- storing a delta from an older snapshot
- compression


## Operations

### Garbage collection

When you run "zenith gc", old timelines that are no longer needed are
removed. That involves collecting the list of "unreachable" objects,
starting from the named branches and tags.

Also, if enough WAL has been generated on a timeline since last
snapshot, a new snapshot or delta is created.

### zenith push/pull

Compare the tags and branches on both servers, and copy missing ones.
For each branch, compare the timeline it points to in both servers. If
one is behind the other, copy the missing parts.

FIXME: how do you prevent confusion if you have to clones of the same
repository, launch an instance on the same branch in both clones, and
later try to push/pull between them? Perhaps create a new timeline
every time you start up an instance? Then you would detect that the
timelines have diverged. That would match with the "epoch" concept
that we have in the WAL safekeepr

### zenith checkout/commit

In this format, there is no concept of a "working tree", and hence no
concept of checking out or committing. All modifications are done on
a branch or a timeline. As soon as you launch a server, the changes are
appended to the timeline.

You can easily fork off a temporary timeline to emulate a "working tree".
You can later remove it and have it garbage collected, or to "commit",
re-point the branch to the new timeline.

If we want to have a worktree and "zenith checkout/commit" concept, we can
emulate that with a temporary timeline. Create the temporary timeline at
"zenith checkout", and have "zenith commit" modify the branch to point to
the new timeline.
