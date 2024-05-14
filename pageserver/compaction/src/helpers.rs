//! This file contains generic utility functions over the interface types,
//! which could be handy for any compaction implementation.
use crate::interface::*;

use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use pageserver_api::shard::ShardIdentity;
use std::collections::VecDeque;
use std::collections::{binary_heap, BinaryHeap};
use std::fmt::Display;
use std::ops::Range;
use utils::lsn::Lsn;

pub const PAGE_SZ: u64 = 8192;

pub fn keyspace_total_size<K>(
    keyspace: &CompactionKeySpace<K>,
    shard_identity: &ShardIdentity,
) -> u64
where
    K: CompactionKey,
{
    keyspace
        .iter()
        .map(|r| K::key_range_size(r, shard_identity) as u64)
        .sum()
}

pub fn overlaps_with<T: Ord>(a: &Range<T>, b: &Range<T>) -> bool {
    !(a.end <= b.start || b.end <= a.start)
}

pub fn union_to_keyspace<K: Ord>(a: &mut CompactionKeySpace<K>, b: CompactionKeySpace<K>) {
    let x = std::mem::take(a);
    let mut all_ranges_iter = [x.into_iter(), b.into_iter()]
        .into_iter()
        .kmerge_by(|a, b| a.start < b.start);
    let mut ranges = Vec::new();
    if let Some(first) = all_ranges_iter.next() {
        let (mut start, mut end) = (first.start, first.end);

        for r in all_ranges_iter {
            assert!(r.start >= start);
            if r.start > end {
                ranges.push(start..end);
                start = r.start;
                end = r.end;
            } else if r.end > end {
                end = r.end;
            }
        }
        ranges.push(start..end);
    }
    *a = ranges
}

pub fn intersect_keyspace<K: Ord + Clone + Copy>(
    a: &CompactionKeySpace<K>,
    r: &Range<K>,
) -> CompactionKeySpace<K> {
    let mut ranges: Vec<Range<K>> = Vec::new();

    for x in a.iter() {
        if x.end <= r.start {
            continue;
        }
        if x.start >= r.end {
            break;
        }
        ranges.push(x.clone())
    }

    // trim the ends
    if let Some(first) = ranges.first_mut() {
        first.start = std::cmp::max(first.start, r.start);
    }
    if let Some(last) = ranges.last_mut() {
        last.end = std::cmp::min(last.end, r.end);
    }
    ranges
}

pub async fn merge_delta_keys_buffered<'a, E: CompactionJobExecutor + 'a>(
    layers: &'a [E::DeltaLayer],
    ctx: &'a E::RequestContext,
) -> anyhow::Result<impl Stream<Item = <E::DeltaLayer as CompactionDeltaLayer<E>>::DeltaEntry<'a>>>
{
    let mut keys = Vec::new();
    for l in layers {
        // Boxing and casting to LoadFuture is required to obtain the right Sync bound.
        // If we do l.load_keys(ctx).await? directly, there is a compilation error.
        let load_future: LoadFuture<'a, _> = Box::pin(l.load_keys(ctx));
        keys.extend(load_future.await?.into_iter());
    }
    keys.sort_by_key(|k| (k.key(), k.lsn()));
    let stream = futures::stream::iter(keys.into_iter());
    Ok(stream)
}

/// Wrapper type to make `dl.load_keys`` compile.
type LoadFuture<'a, E> = BoxFuture<'a, anyhow::Result<Vec<E>>>;

pub enum LayerIterator<'a, E: CompactionJobExecutor> {
    Loaded(
        VecDeque<<E::DeltaLayer as CompactionDeltaLayer<E>>::DeltaEntry<'a>>,
        &'a E::RequestContext,
    ),
    Unloaded(&'a E::DeltaLayer, &'a E::RequestContext),
}

impl<'a, E: CompactionJobExecutor + 'a> LayerIterator<'a, E> {
    pub fn new(delta_layer: &'a E::DeltaLayer, ctx: &'a E::RequestContext) -> Self {
        Self::Unloaded(delta_layer, ctx)
    }

    pub fn key_lsn(&self) -> (E::Key, Lsn) {
        match self {
            Self::Unloaded(dl, _) => (dl.key_range().start, dl.lsn_range().start),
            Self::Loaded(entries, _) => entries.front().map(|x| (x.key(), x.lsn())).unwrap(),
        }
    }

    async fn load(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Unloaded(dl, ctx) => {
                let unloaded_key_lsn = (dl.key_range().start, dl.lsn_range().start);
                let fut: LoadFuture<
                    'a,
                    <E::DeltaLayer as CompactionDeltaLayer<E>>::DeltaEntry<'a>,
                > = Box::pin(dl.load_keys(ctx));
                let keys = VecDeque::from(fut.await?);
                assert_eq!(
                    keys.front().as_ref().map(|x| (x.key(), x.lsn())).unwrap(),
                    unloaded_key_lsn,
                    "unmatched start key_lsn"
                );
                *self = Self::Loaded(keys, ctx);
                Ok(())
            }
            Self::Loaded(_, _) => Ok(()),
        }
    }

    pub async fn entry(
        &mut self,
    ) -> anyhow::Result<&<E::DeltaLayer as CompactionDeltaLayer<E>>::DeltaEntry<'a>> {
        self.load().await?;
        let Self::Loaded(x, _) = self else {
            unreachable!()
        };
        Ok(x.front().unwrap())
    }

    pub async fn next(
        &mut self,
    ) -> anyhow::Result<<E::DeltaLayer as CompactionDeltaLayer<E>>::DeltaEntry<'a>> {
        self.load().await?; // requires Box::pin to make it compile
        let Self::Loaded(x, _) = self else {
            unreachable!()
        };
        Ok(x.pop_front().expect("already reached the end"))
    }

    pub fn is_end(&self) -> bool {
        match self {
            Self::Unloaded(_, _) => false,
            Self::Loaded(x, _) => x.is_empty(),
        }
    }
}

impl<'a, E: CompactionJobExecutor + 'a> PartialOrd for LayerIterator<'a, E> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, E: CompactionJobExecutor + 'a> Ord for LayerIterator<'a, E> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // reverse comparison to get a min-heap
        other.key_lsn().cmp(&self.key_lsn())
    }
}

impl<'a, E: CompactionJobExecutor + 'a> PartialEq for LayerIterator<'a, E> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl<'a, E: CompactionJobExecutor + 'a> Eq for LayerIterator<'a, E> {}

pub struct DeltaMergeIterator<'a, E: CompactionJobExecutor> {
    heap: BinaryHeap<LayerIterator<'a, E>>,
}

impl<'a, E: CompactionJobExecutor + 'a> DeltaMergeIterator<'a, E> {
    pub fn new(delta_layers: &'a [E::DeltaLayer], ctx: &'a E::RequestContext) -> Self {
        let mut heap = BinaryHeap::new();
        for dl in delta_layers {
            heap.push(LayerIterator::new(dl, ctx));
        }
        Self { heap }
    }

    pub fn is_end(&self) -> bool {
        self.heap.is_empty()
    }

    /// The next key-lsn entry that will be returned by `next`.
    pub fn key_lsn(&self) -> (E::Key, Lsn) {
        self.heap.peek().expect("already reached the end").key_lsn()
    }

    /// Move to the next entry and return the current entry.
    pub async fn next(
        &mut self,
    ) -> anyhow::Result<<E::DeltaLayer as CompactionDeltaLayer<E>>::DeltaEntry<'a>> {
        let Some(mut top) = self.heap.peek_mut() else {
            panic!("already reached the end")
        };
        match top.next().await {
            Ok(entry) => {
                if top.is_end() {
                    binary_heap::PeekMut::pop(top);
                }
                Ok(entry)
            }
            Err(e) => {
                // pop the item if there is an error, otherwise it might cause further panic when binary heap compares it after `PeekMut` gets dropped.
                binary_heap::PeekMut::pop(top);
                Err(e)
            }
        }
    }
}

// Accumulate values at key boundaries
pub struct KeySize<K> {
    pub key: K,
    pub num_values: u64,
    pub size: u64,
    /// The lsns to partition at (if empty then no per-lsn partitioning)
    pub partition_lsns: Vec<(Lsn, u64)>,
}

pub fn accum_key_values<'a, I, K, D, E>(
    input: I,
    target_size: u64,
) -> impl Stream<Item = Result<KeySize<K>, E>>
where
    K: Eq + PartialOrd + Display + Copy,
    I: Stream<Item = Result<D, E>>,
    D: CompactionDeltaEntry<'a, K>,
{
    async_stream::try_stream! {
        // Initialize the state from the first value
        let mut input = std::pin::pin!(input);

        if let Some(first) = input.next().await {
            let first = first?;
            let mut part_size = first.size();
            let mut accum: KeySize<K> = KeySize {
                key: first.key(),
                num_values: 1,
                size: part_size,
                partition_lsns: Vec::new(),
            };
            let mut last_key = accum.key;
            while let Some(this) = input.next().await {
                let this = this?;
                if this.key() == accum.key {
                    let add_size = this.size();
                    if part_size + add_size > target_size {
                        accum.partition_lsns.push((this.lsn(), part_size));
                        part_size = 0;
                    }
                    part_size += add_size;
                    accum.size += add_size;
                    accum.num_values += 1;
                } else {
                    assert!(last_key <= accum.key, "last_key={last_key} <= accum.key={}", accum.key);
                    last_key = accum.key;
                    yield accum;
                    part_size = this.size();
                    accum = KeySize {
                        key: this.key(),
                        num_values: 1,
                        size: part_size,
                        partition_lsns: Vec::new(),
                    };
                }
            }
            assert!(last_key <= accum.key, "last_key={last_key} <= accum.key={}", accum.key);
            yield accum;
        }
    }
}
