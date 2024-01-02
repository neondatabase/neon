# Vectored Timeline Get

Created on: 2024-01-02
Author: Christian Schwarz

# Summary

A brief RFC / GitHub Epic describing a vectored version of the `Timeline::get` method that is at the heart of Pageserver.

# Motivation

During basebackup, we issue many `Timeline::get` calls for SLRU pages that are *adjacent* in key space.
For an example, see
https://github.com/neondatabase/neon/blob/5c88213eaf1b1e29c610a078d0b380f69ed49a7e/pageserver/src/basebackup.rs#L281-L302.

Each call must traverse the layer map to gather reconstruct data (`Timeline::get_reconstruct_data`) for the requested page number (`blknum` in the example).

That is quite inefficient because:

1. We do the layer map traversal repeatedly, even if, e.g., all the data sits in the same image layer at the bottom of the stack.
2. Anecdotally, keys adjacent in keyspace and written simultaneously also end up physically adjacent in the layer files [^analysis_slow_basebackup_2023_12_07].
   So, to provide the reconstruct data for N adjacent keys, we would actually only _need_ to issue a single large read to the filesystem, instead of the N reads we currently do. The filesystem in turn ideally stores the data physically contiguously, so, the large read will turn into one IOP.)

[^analysis_slow_basebackup_2023-12_07]: https://www.notion.so/neondatabase/Christian-Investigation-Slow-Basebackups-Early-2023-12-34ea5c7dcdc1485d9ac3731da4d2a6fc?pvs=4#15ee4e143392461fa64590679c8f54c9

# Solution

We should have a vectored aka batched aka scatter-gather style alternative API for `Timeline::get`. Having such an API  unlocks:

* more efficient basebackup
* batched IO during compaction (useful for strides of unchanged pages)
* page_service: expose vectored get_page_at_lsn for compute (=> good for seqscan / prefetch)
  * if [on-demand SLRU downloads](https://github.com/neondatabase/neon/pull/6151) land before vectored Timeline::get, on-demand SLRU downloads will still benefit from this API

# DoD

There is a new variant of `Timeline::get`, called `Timeline::get_vectored`.
It takes as arguments an `lsn: Lsn`, `src: &[KeyVec]` where `struct KeyVec { base: Key, count: usize }`.

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

# Performance

A single invocation of `Timeline::get_vectored` visits each layer at most once.

The base performance is identical to the current `Timeline::get`.

The performance improvement for the vectored use case is demonstrated in some way, e.g., using the `pagebench` basebackup benchmark against a tenant with a lot of SLRU segments.

# Rollout / Feature Flags

No feature flags are required for this epic.

At the end of this epic, `Timeline::get` forwards to `Timeline::get_vectored`, i.e., it's an all-or-nothing type of change.

# Interaction With Sharding

[Sharding](https://github.com/neondatabase/neon/pull/5432) splits up the key space, see functions `is_key_local` / `key_to_shard_number`.

Just as with `Timeline::get`, callers of `Timeline::get_vectored` are responsible for ensuring that they only ask for blocks of the given `struct Timeline`'s shard.

Given that this is already the case, there shouldn't be significant interaction/interference with sharding.

