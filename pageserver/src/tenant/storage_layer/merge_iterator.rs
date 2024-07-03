use std::{
    cmp::Ordering,
    collections::{binary_heap, BinaryHeap},
};

use pageserver_api::key::Key;
use utils::lsn::Lsn;

use crate::{context::RequestContext, repository::Value};

use super::{
    delta_layer::{DeltaLayerInner, DeltaLayerIterator},
    image_layer::{ImageLayerInner, ImageLayerIterator},
};

#[derive(Clone, Copy)]
enum LayerRef<'a> {
    Image(&'a ImageLayerInner),
    Delta(&'a DeltaLayerInner),
}

impl<'a> LayerRef<'a> {
    fn iter(self, ctx: &'a RequestContext) -> LayerIterRef<'a> {
        match self {
            Self::Image(x) => LayerIterRef::Image(x.iter(ctx)),
            Self::Delta(x) => LayerIterRef::Delta(x.iter(ctx)),
        }
    }
}

enum LayerIterRef<'a> {
    Image(ImageLayerIterator<'a>),
    Delta(DeltaLayerIterator<'a>),
}

impl LayerIterRef<'_> {
    async fn next(&mut self) -> anyhow::Result<Option<(Key, Lsn, Value)>> {
        match self {
            Self::Delta(x) => x.next().await,
            Self::Image(x) => x.next().await,
        }
    }
}

enum IteratorWrapper<'a> {
    /// The potential next key of the iterator. If the layer is not loaded yet, it will be the start key encoded in the layer file.
    /// Otherwise, it is the next key of the real iterator.
    NotLoaded {
        ctx: &'a RequestContext,
        first_key_lower_bound: (Key, Lsn),
        layer: LayerRef<'a>,
    },
    Loaded {
        peeked: Option<(Key, Lsn, Value)>, // None == end
        iter: LayerIterRef<'a>,
    },
}

impl<'a> std::cmp::PartialEq for IteratorWrapper<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'a> std::cmp::Eq for IteratorWrapper<'a> {}

impl<'a> std::cmp::PartialOrd for IteratorWrapper<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> std::cmp::Ord for IteratorWrapper<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        let a = self.peek_next_key_lsn();
        let b = other.peek_next_key_lsn();
        match (a, b) {
            (Some((k1, l1)), Some((k2, l2))) => {
                let loaded_1 = if self.is_loaded() { 1 } else { 0 };
                let loaded_2 = if other.is_loaded() { 1 } else { 0 };
                // When key_lsn are the same, the unloaded iter will always appear before the loaded one.
                // And note that we do a reverse at the end of the comparison, so it works with the max heap.
                (k1, l1, loaded_1).cmp(&(k2, l2, loaded_2))
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
        .reverse()
    }
}

impl<'a> IteratorWrapper<'a> {
    pub fn create_from_image_layer(
        image_layer: &'a ImageLayerInner,
        ctx: &'a RequestContext,
    ) -> Self {
        Self::NotLoaded {
            layer: LayerRef::Image(image_layer),
            first_key_lower_bound: (image_layer.key_range().start, image_layer.lsn()),
            ctx,
        }
    }

    pub fn create_from_delta_layer(
        delta_layer: &'a DeltaLayerInner,
        ctx: &'a RequestContext,
    ) -> Self {
        Self::NotLoaded {
            layer: LayerRef::Delta(delta_layer),
            first_key_lower_bound: (delta_layer.key_range().start, delta_layer.lsn_range().start),
            ctx,
        }
    }

    fn peek_next_key_lsn(&self) -> Option<(&Key, Lsn)> {
        match self {
            Self::Loaded {
                peeked: Some((key, lsn, _)),
                ..
            } => Some((key, *lsn)),
            Self::Loaded { peeked: None, .. } => None,
            Self::NotLoaded {
                first_key_lower_bound: (key, lsn),
                ..
            } => Some((key, *lsn)),
        }
    }

    // CORRECTNESS: this function must always take `&mut self`, never `&self`.
    //
    // The reason is that `impl Ord for Self` evaluates differently after this function
    // returns. We're called through a `PeekMut::deref_mut`, which causes heap repair when
    // the PeekMut gets returned. So, it's critical that we actually run through `PeekMut::deref_mut`
    // and not just `PeekMut::deref`
    // If we don't take `&mut self`
    async fn load(&mut self) -> anyhow::Result<()> {
        assert!(!self.is_loaded());
        let Self::NotLoaded {
            ctx,
            first_key_lower_bound,
            layer,
        } = self
        else {
            unreachable!()
        };
        let mut iter = layer.iter(ctx);
        let peeked = iter.next().await?;
        if let Some((k1, l1, _)) = &peeked {
            let (k2, l2) = first_key_lower_bound;
            debug_assert!((k1, l1) >= (k2, l2));
        }
        *self = Self::Loaded { peeked, iter };
        Ok(())
    }

    fn is_loaded(&self) -> bool {
        matches!(self, Self::Loaded { .. })
    }

    /// Correctness: must load the iterator before using.
    ///
    /// Given this iterator wrapper is private to the merge iterator, users won't be able to mis-use it.
    /// The public interfaces to use are [`crate::tenant::storage_layer::delta_layer::DeltaLayerIterator`] and
    /// [`crate::tenant::storage_layer::image_layer::ImageLayerIterator`].
    async fn next(&mut self) -> anyhow::Result<Option<(Key, Lsn, Value)>> {
        let Self::Loaded { peeked, iter } = self else {
            panic!("must load the iterator before using")
        };
        let result = peeked.take();
        *peeked = iter.next().await?;
        Ok(result)
    }
}

pub struct MergeIterator<'a> {
    heap: BinaryHeap<IteratorWrapper<'a>>,
}

impl<'a> MergeIterator<'a> {
    pub fn create(
        deltas: &[&'a DeltaLayerInner],
        images: &[&'a ImageLayerInner],
        ctx: &'a RequestContext,
    ) -> Self {
        let mut heap = Vec::with_capacity(images.len() + deltas.len());
        for image in images {
            heap.push(IteratorWrapper::create_from_image_layer(image, ctx));
        }
        for delta in deltas {
            heap.push(IteratorWrapper::create_from_delta_layer(delta, ctx));
        }
        Self {
            heap: BinaryHeap::from(heap),
        }
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<(Key, Lsn, Value)>> {
        while let Some(mut iter) = self.heap.peek_mut() {
            if !iter.is_loaded() {
                iter.load().await?;
            } else {
                let res = iter.next().await?;
                if res.is_none() {
                    binary_heap::PeekMut::pop(iter);
                }
                return Ok(res);
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use itertools::Itertools;
    use pageserver_api::key::Key;
    use utils::lsn::Lsn;

    use crate::{
        tenant::{
            harness::{TenantHarness, TIMELINE_ID},
            storage_layer::delta_layer::test::{produce_delta_layer, sort_delta},
        },
        DEFAULT_PG_VERSION,
    };

    async fn assert_merge_iter_equal(
        merge_iter: &mut MergeIterator<'_>,
        expect: &[(Key, Lsn, Value)],
    ) {
        let mut expect_iter = expect.iter();
        loop {
            let o1 = merge_iter.next().await.unwrap();
            let o2 = expect_iter.next();
            assert_eq!(o1.is_some(), o2.is_some());
            if o1.is_none() && o2.is_none() {
                break;
            }
            let (k1, l1, v1) = o1.unwrap();
            let (k2, l2, v2) = o2.unwrap();
            assert_eq!(&k1, k2);
            assert_eq!(l1, *l2);
            assert_eq!(&v1, v2);
        }
    }

    #[tokio::test]
    async fn delta_merge() {
        use crate::repository::Value;
        use bytes::Bytes;

        let harness = TenantHarness::create("merge_iterator_delta_merge").unwrap();
        let (tenant, ctx) = harness.load().await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        fn get_key(id: u32) -> Key {
            let mut key = Key::from_hex("000000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }
        const N: usize = 1000;
        let test_deltas1 = (0..N)
            .map(|idx| {
                (
                    get_key(idx as u32 / 10),
                    Lsn(0x20 * ((idx as u64) % 10 + 1)),
                    Value::Image(Bytes::from(format!("img{idx:05}"))),
                )
            })
            .collect_vec();
        let resident_layer_1 = produce_delta_layer(&tenant, &tline, test_deltas1.clone(), &ctx)
            .await
            .unwrap();
        let test_deltas2 = (0..N)
            .map(|idx| {
                (
                    get_key(idx as u32 / 10),
                    Lsn(0x20 * ((idx as u64) % 10 + 1) + 0x10),
                    Value::Image(Bytes::from(format!("img{idx:05}"))),
                )
            })
            .collect_vec();
        let resident_layer_2 = produce_delta_layer(&tenant, &tline, test_deltas2.clone(), &ctx)
            .await
            .unwrap();
        let test_deltas3 = (0..N)
            .map(|idx| {
                (
                    get_key(idx as u32 / 10 + N as u32),
                    Lsn(0x10 * ((idx as u64) % 10 + 1)),
                    Value::Image(Bytes::from(format!("img{idx:05}"))),
                )
            })
            .collect_vec();
        let resident_layer_3 = produce_delta_layer(&tenant, &tline, test_deltas3.clone(), &ctx)
            .await
            .unwrap();
        let mut merge_iter = MergeIterator::create(
            &[
                resident_layer_1.get_as_delta(&ctx).await.unwrap(),
                resident_layer_2.get_as_delta(&ctx).await.unwrap(),
                resident_layer_3.get_as_delta(&ctx).await.unwrap(),
            ],
            &[],
            &ctx,
        );
        let mut expect = Vec::new();
        expect.extend(test_deltas1);
        expect.extend(test_deltas2);
        expect.extend(test_deltas3);
        expect.sort_by(sort_delta);
        assert_merge_iter_equal(&mut merge_iter, &expect).await;

        // TODO: test layers are loaded only when needed, reducing num of active iterators in k-merge
    }

    // TODO: image layer merge, delta+image mixed merge
    // TODO: is it possible to have duplicated delta at same LSN now? we might need to test that
}
