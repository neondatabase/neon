# Preface

GetPage@LSN can be called with older LSNs, and the page server needs
to be able to reconstruct older page versions. That's needed for
having read-only replicas that lag behind the primary, or that are
"anchored" at an older LSN, and internally in the page server when you
branch at an older point in time. How do you do that?

For now, I'm not considering incremental snapshots at all. I don't
think that changes things. So whenever you create a snapshot or a
snapshot file, it contains an image of all the pages, there is no need
to look at an older snapshot file.

Also, I'm imagining that this works on a per-relation basis, so that
each snapshot file contains data for one relation. A "relation" is a
fuzzy concept - it could actually be one 1 GB relation segment. Or it
could include all the different "forks" of a relation, or you could
treat each fork as a separate relation for storage purpose. And once
we have the "non-relational" work is finished, a "relation" could
actually mean some other versioned object kept in the PostgreSQL data
directory. Let's ignore that for now.

# Eric's RFC:

Every now and then, you create a "snapshot". It means that you create
a new snapshot file for each relation that was modified after the last
snapshot, and write out the contents the relation as it is/was at the
snapshot LSN. Write-ahead log is stored separately in S3 by the WAL
safekeeping service, in the original PostgreSQL WAL file format.

    SNAPSHOT @100       WAL
       .                 |
       .                 |
       .                 |
       .                 |
    SNAPSHOT @200        |
       .                 |
       .                 |
       .                 |
       .                 |
    SNAPSHOT @300        |
       .                 |
       .                 V
    IN-MEMORY @400

If a GetPage@LSN request comes from the primary, you return the latest
page from the in-memory layer. If there is no trace of the page in
memory, it means that it hasn't been modified since the last snapshot,
so you return the page from the latest snapshot, at LSN 300 in the
above example.

PITR is implemented using the original WAL files:

If a GetPage@LSN request comes from a read replica with LSN 250, you
read the image of the page from the snapshot at LSN 200, and you also
scan the WAL between 200 and 250, and apply all WAL records for the
requested page, to reconstruct it at LSN 250.

Scanning the WAL naively for every GetPage@LSN request would be
expensive, so in practice you'd construct an in-memory data structure
of all the WAL between 200 and 250 once that allows quickly looking up
records for a given page.

## Problems/questions

I think you'll need to store the list of snapshot LSNs on each
timeline somewhere.

If the latest snapshot of a relation is at LSN 100, and you request a
page at LSN 1000000, how do you know if there are some modifications
to it between 100 and 1000000 that you need to replay? You can scan
all the WAL between 100 and 1000000, but that would be expensive.

You can skip that, if you know that a snapshot was taken e.g. at LSN
999900. Then you know that the fact that there is no snapshot file at
999900 means that the relation hasn't been modified between
100-999900.  Then you only need to scan the WAL between 999900 and
1000000. However, there is no trace of a snapshot happening at LSN
999900 in the snapshot file for this relation, so you need to get
that information from somewhere else.

Where do you get that information from? Perhaps you can scan all the
other relations, and if you see a snapshot file for *any* relation at
LSN 999900, you know that if there were modifications to this
relation, there would be a newer snapshot file for it, too. In other
words, the list of snapshots that have been taken can be constructed
by scanning all relations and computing the union of all snapshot LSNs
that you see for any relation. But that's expensive so at least you
should keep that in memory, after computing it once. Also, if you rely
on that, it's not possible to have snapshots at different intervals
for different files. That seems limiting.

Another option is to explicitly store a list of snapshot LSNs in a
separate metadata file.


# Current implementation in the 'layered_repo' branch:

We store snapshot files like in the RFC, but each snapshot file also
contains all the WAL in the range of LSNs, so that you don't need to
fetch the WAL separately from S3. So you have "layers" like this:

    SNAPSHOT+WAL 100-200
          |
          |
          |
          |
    SNAPSHOT+WAL 200-300
          |
          |
          |
          |
    IN-MEMORY 300-

Each "snapshot+WAL" is a file that contains a snapshot - i.e. full
copy of each page in the relation, at the *start* LSN. In addition to
that, it contains all the WAL applicable to the relation from the
start LSN to the end LSN. With that, you can reconstruct any page
version in the range that the file covers.


## Problems/questions

I can see one potential performance issue here, compared to the RFC.
Let's focus on a single relation for now. Imagine that you start from
an empty relation, and you receive WAL from 100 to 200, containing
a bunch of inserts and updates to the relation. You now have all that
WAL in memory:

    memory:  WAL from 100-200

We decide that it's time to materialize that to a snapshot file on
disk.  We materialize full image of the relation as it was at LSN 100
to the snapshot file, and include all of the WAL. Since the relation
was initially empty, the "image" at the beginning of th range is empty
too.

So now you have one file on on disk:

    SNAPSHOT+WAL 100-200

It contains a full image of the relation at LSN 100 and all WAL
between 100-200. (It's actually stored as a serialized BTreeMap of
page versions, with the page images and WAL records all stored
together in the same BtreeMap. But for this story, that's not
important.)

We now receive more WAL updating the relation, up to LSN 300. We
decide it's time to materialize a new snapshot file, and we now have
two files:

    SNAPSHOT+WAL 100-200
    SNAPSHOT+WAL 200-300

Note that the latest "full snapshot" that we store on disk always lags
behind by one snapshot cycle. The first file contains a full image of
the relation at LSN 100, the second at LSN 200. When we have received
WAL up to LSN 300, we write a materialized image at LSN 200. That
seems a bit silly. In the design per your RFC, you would write a
snapshots at LSNs 200 and 300, instead. That seems better.



# Third option (not implemented yet)

Store snapshot files like in the RFC, but also store per-relation
WAL files that contain WAL in a range of LSNs for that relation.

    SNAPSHOT @100   WAL 100-200
       .                 |
       .                 |
       .                 |
       .                 |
    SNAPSHOT @200   WAL 200-300
       .                 |
       .                 |
       .                 |
       .                 |
    SNAPSHOT @300
       .
       .
    IN-MEMORY 300-


This could be the best of both worlds. The snapshot files would be
independent of the PostgreSQL WAL format. When it's time to write
snapshot file @300, you write a full image of the relation at LSN 300,
and you write the WAL that you had accumulated between 200 and 300 to
a separate file. That way, you don't "lag behind" for one snapshot
cycle like in the current implementation. But you still have the WAL
for a particular relation readily available alongside the snapshot
files, and you don't need to track what snapshot LSNs exist
separately.

(If we wanted to minimize the number of files, you could include the
snapshot @300 and the WAL between 200 and 300 in the same file, but I
feel it's probably better to keep them separate)



# Further thoughts

There's no fundamental reason why the LSNs of the snapshot files and the
ranges of the WAL files would need to line up. So this would be possible
too:

    SNAPSHOT @100   WAL 100-150
       .                 |
       .                 |
       .            WAL 150-250
       .                 |
    SNAPSHOT @200        |
       .                 |
       .            WAL 250-400
       .                 |
       .                 |
    SNAPSHOT @300        |
       .                 |
       .                 |
    IN-MEMORY 300-

I'm not sure what the benefit of this would be. You could materialize
additional snapshot files in the middle of a range covered by a WAL
file, maybe? Might be useful to speed up access when you create a new
branch in the middle of an LSN range or if there's some other reason
to believe that a particular LSN is "interesting" and there will be
a lot of requests using it.
