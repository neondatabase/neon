# User-visible timeline history

The user can specify a retention policy. The retention policy is
presented to the user as a PITR period and snapshots. The PITR period
is the amount of recent history that needs to be retained, as minutes,
hours, or days. Within that period, you can create a branch or
snapshot at any point in time, open a compute node, and start running
queries. Internally, a PITR period is represented as a range of LSNs

The user can also create snapshots. A snapshot is a point in time,
internally represented by an LSN. The user gives the snapshot a name.

The user can also specify an interval, at which the system creates
snapshots automatically. For example, create a snapshot every night at
2 AM. After some user-specified time, old automatically created
snapshots are removed.

                     Snapshot       Snapshot
         PITR        "Monday"       "Tuesday"        PITR
    ----######----------+-------------+-------------######>

If there are multiple branches, you can specify different policies or
different branches.

The PITR period and user-visible snapshots together define the
retention policy.

NOTE: As presented here, this is probably overly flexible. In reality,
we want to keep the user interface simple. Only allow a PITR period at
the tip of a branch, for example. But that doesn't make much
difference to the internals.


# Retention policy behind the scenes

The retention policy consists of points (for snapshots) and ranges
(for PITR periods).

The system must be able to reconstruct any page within the retention
policy. Other page versions can be garbage collected away. We have a
lot of flexibility on when to perform the garbage collection and how
aggressive it is.


# Base images and WAL slices

The page versions are stored in two kinds of files: base images and
WAL slices. A base image contains a dump of all the pages of one
relation at a specific LSN. A WAL slice contains all the WAL in an LSN
range.


    |
    |
    |
    | --Base img @100   +
    |                   |
    |                   | WAL slice
    |                   | 100-200
    |                   |
    | --Base img @200   +
    |                   |
    |                   | WAL slice
    |                   | 200-300
    |                   |
    |                   +
    |
    V


To recover a page e.g. at LSN 150, you need the base image at LSN 100,
and the WAL slice 100-200.

All of this works at a per-relation or per-relation-segment basis. If
a relation is updated very frequently, we create base images and WAL
slices for it more quickly. For a relation that's updated
infrequently, we hold the recent WAL for that relation longer, and
only write it out when we need to release the disk space occupied by
the original WAL. (We need a backstop like that, because until all the
WAL/base images have been been durably copied to S3, we must keep the
original WAL for that period somewhere, in the WAL service or in S3.)


# Branching

Internally, branch points are also "retention points", in addition to
the user-visible snapshots. If a branch has been forked off at LSN
100, we need to be able to reconstruct any page on the parent branch
at that LSN, because it is needed by the child branch. If a page is
modified in the child, we don't need to keep that in the parent
anymore, though.
