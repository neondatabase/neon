# Eviction

 Write out in-memory layer to disk, into a delta layer.

- To release memory
- To make it possible to advance disk_consistent_lsn and allow the WAL
  service to release some WAL.

- Triggered if we are short on memory
- Or if the oldest in-memory layer is so old that it's holding back
  the WAL service from removing old WAL

# Materialization

Create a new image layer of a segment, by performing WAL redo

- To reduce the amount of WAL that needs to be replayed on a GetPage request.
- To allow garbage collection of old layers

- Triggered by distance to last full image of a page

# Coalescing

Replace N consecutive layers of a segment with one larger layer.

- To reduce the number of small files that needs to be uploaded to S3


# Bundling

Zip together multiple small files belonging to different segments.

- To reduce the number of small files that needs to be uploaded to S3


# Garbage collection

Remove a layer that's older than the GC horizon, and isn't needed anymore.
