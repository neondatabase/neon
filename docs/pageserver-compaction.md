# Pageserver Compaction

Lifted from <https://www.notion.so/neondatabase/Rough-Notes-on-Compaction-1baf189e004780859e65ef63b85cfa81?pvs=4>.

Updated 2025-03-26.

## Pages and WAL

Postgres stores data in 8 KB pages, identified by a page number.

The WAL contains a sequence of page writes: either images (complete page contents) or deltas (patches applied to images). Each write is identified by its byte position in the WAL, aka LSN. 

Each page version is thus identified by page@LSN. Postgres may read pages at past LSNs.

Pageservers ingest WAL by writing WAL records into a key/value store keyed by page@LSN.

Pageservers materialize pages for Postgres reads by finding the most recent page image and applying all subsequent page deltas, up to the read LSN.

## Compaction: Why?

Pageservers store page@LSN keys in a key/value store using a custom variant of an LSM tree. Each timeline on each tenant shard has its own LSM tree.

When Pageservers write new page@LSN entries, they are appended unordered to an ephemeral layer file. When the ephemeral layer file exceeds `checkpoint_distance` (default 256 MB), the key/value pairs are sorted by key and written out to a layer file (for efficient lookups).

As WAL writes continue, more layer files accumulate.

Reads must search through the layer files to find the page’s image and deltas. The more layer files accumulate, the more la yer files reads must search through before they find a page image, aka read amplification.

Compaction’s job is to:

- Reduce read amplification by reorganizing and combining layer files.
- Remove old garbage from layer files.

As part of this, it may combine several page deltas into a single page image where possible.

## Compaction: How?

Neon uses a non-standard variant of an LSM tree made up of two levels of layer files: L0 and L1.

Compaction runs in two phases: L0→L1 compaction, and L1 image compaction.

L0 contains a stack of L0 layers at decreasing LSN ranges. These have been flushed sequentially from ephemeral layers. Each L0 layer covers the entire page space (page 0 to ~infinity) and the LSN range that was ingested into it. L0 layers are therefore particularly bad for read amp, since every read must search all L0 layers below the read LSN. For example:

```
| Page 0-99 @ LSN 0400-04ff |
| Page 0-99 @ LSN 0300-03ff |
| Page 0-99 @ LSN 0200-02ff |
| Page 0-99 @ LSN 0100-01ff |
| Page 0-99 @ LSN 0000-00ff |
```

L0→L1 compaction takes the bottom-most chunk of L0 layer files of between `compaction_threshold` (default 10) and `compaction_upper_limit` (default 20) layers. It uses merge-sort to write out sorted L1 delta layers of size `compaction_target_size` (default 128 MB).

L1 typically consists of a “bed” of image layers with materialized page images at a specific LSN, and then delta layers of various page/LSN ranges above them with page deltas. For example:

```
Delta layers:               |     30-84@0310-04ff      |
Delta layers:    | 10-42@0200-02ff |           | 65-92@0174-02aa |
Image layers: |    0-39@0100    |    40-79@0100    |    80-99@0100    |
```

L1 image compaction scans across the L1 keyspace at some LSN, materializes page images by reading the image and delta layers below the LSN (via vectored reads), and writes out new sorted image layers of roughly size `compaction_target_size` (default 128 MB) at that LSN.

Layer files below the new image files’ LSN can be garbage collected when they are no longer needed for PITR.

Even though the old layer files are not immediately garbage collected, the new image layers help with read amp because reads can stop traversing the layer stack as soon as they encounter a page image.

## Compaction: When?

Pageservers run a `compaction_loop` background task for each tenant shard. Every `compaction_period` (default 20 seconds) it will wake up and check if any of the shard’s timelines need compaction. Additionally, L0 layer flushes will eagerly wake the compaction loop if the L0 count exceeds `compaction_threshold` (default 10).

L0 compaction runs if the number of L0 layers exceeds `compaction_threshold` (default 10).

L1 image compaction runs across sections of the L1 keyspace that have at least `image_creation_threshold` (default 3) delta layers overlapping image layers.

At most `CONCURRENT_BACKGROUND_TASKS` (default 3 / 4 * CPUs = 6) background tasks can run concurrently on a Pageserver, including compaction. Further compaction tasks must wait.

Because L0 layers cause the most read amp (they overlap the entire keyspace and only contain page deltas), they are aggressively compacted down:

- L0 is compacted down across all tenant timelines before L1 compaction is attempted (`compaction_l0_first`).
- L0 compaction uses a separate concurrency limit of `CONCURRENT_L0_COMPACTION_TASKS` (default 3 / 4 * CPUs = 6) to avoid waiting for other tasks (`compaction_l0_semaphore`).
- If L0 compaction is needed on any tenant timeline, L1 image compaction will yield to start an immediate L0 compaction run (except for compaction run via admin APIs).

## Backpressure

With sustained heavy write loads, new L0 layers may be flushed faster than they can be compacted down. This can cause an unbounded buildup of read amplification and compaction debt, which can take hours to resolve even after the writes stop.

To avoid this and allow compaction to keep up, layer flushes will slow writes down to apply backpressure on the workload:

- At `l0_flush_delay_threshold` (default 30) L0 layers, layer flushes are delayed by the flush duration, such that they take 2x as long.
- At `l0_flush_stall_threshold` (default disabled) L0 layers, layer flushes stall entirely until the L0 count falls back below the threshold. This is currently disabled because we don’t trust L0 compaction to be responsive enough.

This backpressure is propagated to the compute by waiting for layer flushes when WAL ingestion rolls the ephemeral layer. The compute will significantly slow down WAL writes at:

- `max_replication_write_lag` (default 500 MB), when Pageserver WAL ingestion lags
- `max_replication_flush_lag` (default 10 GB), when Pageserver L0 flushes lag

Combined, this means that the compute will backpressure when there are 30 L0 layers (30 * 256 MB = 7.7 GB) and the Pageserver WAL ingestion lags the compute by 500 MB, for a total of ~8 GB L0+ephemeral compaction debt on a single shard.

Since we only delay L0 flushes by 2x when backpressuring, and haven’t enabled stalls, it is still possible for read amp to increase unbounded if compaction is too slow (although we haven’t seen this in practice). But this is considered better than stalling flushes and causing unavailability for as long as it takes L0 compaction to react, since we don’t trust it to be fast enough — at the expense of continually increasing read latency and CPU usage for this tenant. We should either enable stalls when we have enough confidence in L0 compaction, or scale the flush delay by the number of L0 layers to apply increasing backpressure.

## Circuit Breaker

Compaction can fail, often repeatedly. This can happen e.g. due to data corruption, faulty hardware, S3 outages, etc.

If compaction fails, the compaction loop will naïvely try and fail again almost immediately. It may only fail after doing a significant amount of wasted work, while holding onto the background task semaphore.

To avoid repeatedly doing wasted work and starving out other compaction jobs, each tenant has a compaction circuit breaker. After 5 repeated compaction failures, the circuit breaker trips and disables compaction for the next 24 hours, before resetting the breaker and trying again. This disables compaction across all tenant timelines (faulty or not).

Disabling compaction for a long time is dangerous, since it can lead to unbounded read amp and compaction debt, and continuous workload backpressure. However, continually failing would not help either. Tripped circuit breakers trigger an alert and must be investigated promptly.