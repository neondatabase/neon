# Vectored Timeline Get

Created on: 2024-01-02
Author: Christian Schwarz

# Summary

A brief RFC / GitHub Epic describing a vectored version of the `Timeline::get` method that is at the heart of Pageserver.

# Motivation

During basebackup, we issue many `Timeline::get` calls for SLRU pages that are *adjacent* in key space.
For an example, see
https://github.com/neondatabase/neon/blob/5c88213eaf1b1e29c610a078d0b380f69ed49a7e/pageserver/src/basebackup.rs#L281-L302.

Each of these `Timeline::get` calls must traverse the layer map to gather reconstruct data (`Timeline::get_reconstruct_data`) for the requested page number (`blknum` in the example).
For each layer visited by layer map traversal, we do a `DiskBtree` point lookup.
If it's negative (no entry), we resume layer map traversal.
If it's positive, we collect the result in our reconstruct data bag.
If the reconstruct data bag contents suffice to reconstruct the page, we're done with `get_reconstruct_data` and move on to walredo.
Otherwise, we resume layer map traversal.

Doing this many `Timeline::get` calls is quite inefficient because:

1. We do the layer map traversal repeatedly, even if, e.g., all the data sits in the same image layer at the bottom of the stack.
2. We may visit many DiskBtree inner pages multiple times for point lookup of different keys.
   This is likely particularly bad for L0s which span the whole key space and hence must be visited by layer map traversal, but
   may not contain the data we're looking for.
3. Anecdotally, keys adjacent in keyspace and written simultaneously also end up physically adjacent in the layer files [^1].
   So, to provide the reconstruct data for N adjacent keys, we would actually only _need_ to issue a single large read to the filesystem, instead of the N reads we currently do.
   The filesystem, in turn, ideally stores the layer file physically contiguously, so our large read will turn into one IOP toward the disk.

[^1]: https://www.notion.so/neondatabase/Christian-Investigation-Slow-Basebackups-Early-2023-12-34ea5c7dcdc1485d9ac3731da4d2a6fc?pvs=4#15ee4e143392461fa64590679c8f54c9

# Solution

We should have a vectored aka batched aka scatter-gather style alternative API for `Timeline::get`. Having such an API  unlocks:

* more efficient basebackup
* batched IO during compaction (useful for strides of unchanged pages)
* page_service: expose vectored get_page_at_lsn for compute (=> good for seqscan / prefetch)
  * if [on-demand SLRU downloads](https://github.com/neondatabase/neon/pull/6151) land before vectored Timeline::get, on-demand SLRU downloads will still benefit from this API

# DoD

There is a new variant of `Timeline::get`, called `Timeline::get_vectored`.
It takes as arguments an `lsn: Lsn` and a `src: &[KeyVec]` where `struct KeyVec { base: Key, count: usize }`.

It is up to the implementor to figure out a suitable and efficient way to return the reconstructed page images.
It is sufficient to simply return a `Vec<Bytes>`, but, likely more efficient solutions can be found after studying all the callers of `Timeline::get`.

Functionally, the behavior of `Timeline::get_vectored` is equivalent to

```rust
let mut keys_iter: impl Iterator<Item=Key>
  = src.map(|KeyVec{ base, count }| (base..base+count)).flatten();
let mut out = Vec::new();
for key in keys_iter {
    let data = Timeline::get(key, lsn)?;
    out.push(data);
}
return out;
```

However, unlike above, an ideal solution will

* Visit each `struct Layer` at most once.
* For each visited layer, call `Layer::get_value_reconstruct_data` at most once.
  * This means, read each `DiskBtree` page at most once.
* Facilitate merging of the reads we issue to the OS and eventually NVMe.

Each of these items above represents a significant amount of work.

## Performance

Ideally, the **base performance** of a vectored get of a single page should be identical to the current `Timeline::get`.
A reasonable constant overhead over current `Timeline::get` is acceptable.

The performance improvement for the vectored use case is demonstrated in some way, e.g., using the `pagebench` basebackup benchmark against a tenant with a lot of SLRU segments.

# Implementation

High-level set of tasks / changes to be made:

- **Get clarity on API**:
  - Define naive `Timeline::get_vectored` implementation & adopt it across pageserver.
  - The tricky thing here will be the return type (e.g. `Vec<Bytes>` vs `impl Stream`).
  - Start with something simple to explore the different usages of the API.
    Then iterate with peers until we have something that is good enough.
- **Vectored Layer Map traversal**
  - Vectored `LayerMap::search` (take 1 LSN and N `Key`s instead of just 1 LSN and 1 `Key`)
  - Refactor `Timeline::get_reconstruct_data` to hold & return state for N `Key`s instead of 1
    - The slightly tricky part here is what to do about `cont_lsn` [after we've found some reconstruct data for some keys](https://github.com/neondatabase/neon/blob/d066dad84b076daf3781cdf9a692098889d3974e/pageserver/src/tenant/timeline.rs#L2378-L2385)
      but need more.
      Likely we'll need to keep track of `cont_lsn` per key and continue next iteration at `max(cont_lsn)` of all keys that still need data.
- **Vectored `Layer::get_value_reconstruct_data` / `DiskBtree`**
  - Current code calls it [here](https://github.com/neondatabase/neon/blob/d066dad84b076daf3781cdf9a692098889d3974e/pageserver/src/tenant/timeline.rs#L2378-L2384).
  - Delta layers use `DiskBtreeReader::visit()` to collect the `(offset,len)` pairs for delta record blobs to load.
  - Image layers use `DiskBtreeReader::get` to get the offset of the image blob to load. Underneath, that's just a `::visit()` call.
  - What needs to happen to `DiskBtree::visit()`?
    * Minimally
      * take a single `KeyVec` instead of a single `Key` as argument, i.e., take a single contiguous key range to visit.
      * Change the visit code to to invoke the callback for all values in the `KeyVec`'s key range
      * This should be good enough for what we've seen when investigating basebackup slowness, because there, the key ranges are contiguous.
    * Ideally:
      * Take a `&[KeyVec]`, sort it;
      * during Btree traversal, peek at the next `KeyVec` range to determine whether we need to descend or back out.
      * NB: this should be a straight-forward extension of the minimal solution above, as we'll already be checking for "is there more key range in the requested `KeyVec`".
- **Facilitate merging of the reads we issue to the OS and eventually NVMe.**
  - The `DiskBtree::visit` produces a set of offsets which we then read from a `VirtualFile` [here](https://github.com/neondatabase/neon/blob/292281c9dfb24152b728b1a846cc45105dac7fe0/pageserver/src/tenant/storage_layer/delta_layer.rs#L772-L804)
    - [Delta layer reads](https://github.com/neondatabase/neon/blob/292281c9dfb24152b728b1a846cc45105dac7fe0/pageserver/src/tenant/storage_layer/delta_layer.rs#L772-L804)
      - We hit (and rely) on `PageCache` and `VirtualFile here (not great under pressure)
    - [Image layer reads](https://github.com/neondatabase/neon/blob/292281c9dfb24152b728b1a846cc45105dac7fe0/pageserver/src/tenant/storage_layer/image_layer.rs#L429-L435)
  - What needs to happen is the **vectorization of the `blob_io` interface and then the `VirtualFile` API**.
  - That is tricky because
    - the `VirtualFile` API, which sits underneath `blob_io`, is being touched by ongoing [io_uring work](https://github.com/neondatabase/neon/pull/5824)
    - there's the question how IO buffers will be managed; currently this area relies heavily on `PageCache`, but there's controversy around the future of `PageCache`.
      - The guiding principle here should be to avoid coupling this work to the `PageCache`.
      - I.e., treat `PageCache` as an extra hop in the I/O chain, rather than as an integral part of buffer management.


Let's see how we can improve by doing the first three items in above list first, then revisit.

## Rollout / Feature Flags

No feature flags are required for this epic.

At the end of this epic, `Timeline::get` forwards to `Timeline::get_vectored`, i.e., it's an all-or-nothing type of change.

It is encouraged to deliver this feature incrementally, i.e., do many small PRs over multiple weeks.
That will help isolate performance regressions across weekly releases.

# Interaction With Sharding

[Sharding](https://github.com/neondatabase/neon/pull/5432) splits up the key space, see functions `is_key_local` / `key_to_shard_number`.

Just as with `Timeline::get`, callers of `Timeline::get_vectored` are responsible for ensuring that they only ask for blocks of the given `struct Timeline`'s shard.

Given that this is already the case, there shouldn't be significant interaction/interference with sharding.

However, let's have a safety check for this constraint (error or assertion) because there are currently few affordances at the higher layers of Pageserver for sharding<=>keyspace interaction.
For example, `KeySpace` is not broken up by shard stripe, so if someone naively converted the compaction code to issue a vectored get for a keyspace range it would violate this constraint.
