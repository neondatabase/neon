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

/// This type plays several roles at once
/// 1. Unified iterator for image and delta layers.
/// 2. `Ord` for use in [`MergeIterator::heap`] (for the k-merge).
/// 3. Lazy creation of the real delta/image iterator.
enum IteratorWrapper<'a> {
    NotLoaded {
        ctx: &'a RequestContext,
        first_key_lower_bound: (Key, Lsn),
        layer: LayerRef<'a>,
    },
    Loaded {
        iter: PeekableLayerIterRef<'a>,
    },
}

struct PeekableLayerIterRef<'a> {
    iter: LayerIterRef<'a>,
    peeked: Option<(Key, Lsn, Value)>, // None == end
}

impl<'a> PeekableLayerIterRef<'a> {
    async fn create(mut iter: LayerIterRef<'a>) -> anyhow::Result<Self> {
        let peeked = iter.next().await?;
        Ok(Self { iter, peeked })
    }

    fn peek(&self) -> &Option<(Key, Lsn, Value)> {
        &self.peeked
    }

    async fn next(&mut self) -> anyhow::Result<Option<(Key, Lsn, Value)>> {
        let result = self.peeked.take();
        self.peeked = self.iter.next().await?;
        Ok(result)
    }
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
        let a = self.peek_next_key_lsn_value();
        let b = other.peek_next_key_lsn_value();
        match (a, b) {
            (Some((k1, l1, v1)), Some((k2, l2, v2))) => {
                fn map_value_to_num(val: &Option<&Value>) -> usize {
                    match val {
                        None => 0,
                        Some(Value::Image(_)) => 1,
                        Some(Value::WalRecord(_)) => 2,
                    }
                }
                let order_1 = map_value_to_num(&v1);
                let order_2 = map_value_to_num(&v2);
                // When key_lsn are the same, the unloaded iter will always appear before the loaded one.
                // And note that we do a reverse at the end of the comparison, so it works with the max heap.
                (k1, l1, order_1).cmp(&(k2, l2, order_2))
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

    fn peek_next_key_lsn_value(&self) -> Option<(&Key, Lsn, Option<&Value>)> {
        match self {
            Self::Loaded { iter } => iter
                .peek()
                .as_ref()
                .map(|(key, lsn, val)| (key, *lsn, Some(val))),
            Self::NotLoaded {
                first_key_lower_bound: (key, lsn),
                ..
            } => Some((key, *lsn, None)),
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
        let iter = layer.iter(ctx);
        let iter = PeekableLayerIterRef::create(iter).await?;
        if let Some((k1, l1, _)) = iter.peek() {
            let (k2, l2) = first_key_lower_bound;
            debug_assert!((k1, l1) >= (k2, l2));
        }
        *self = Self::Loaded { iter };
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
        let Self::Loaded { iter } = self else {
            panic!("must load the iterator before using")
        };
        iter.next().await
    }
}

/// A merge iterator over delta/image layer iterators. When duplicated records are
/// found, the iterator will return everything as-is, and the caller should handle
/// these situation. By saying duplicated records, there are many possibilities:
/// * Two same delta at the same LSN.
/// * Two same image at the same LSN.
/// * Delta/image at the same LSN where the image has already applied the delta.
/// The iterator will always put the image before the delta.
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
                // Once we load the iterator, we can know the real first key-value pair in the iterator.
                // We put it back into the heap so that a potentially unloaded layer may have a key between
                // [potential_first_key, loaded_first_key).
                iter.load().await?;
                continue;
            }
            let Some(item) = iter.next().await? else {
                // If the iterator returns None, we pop this iterator. Actually, in the current implementation,
                // we order None > Some, and all the rest of the iterators should return None.
                binary_heap::PeekMut::pop(iter);
                continue;
            };
            return Ok(Some(item));
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
            storage_layer::delta_layer::test::{produce_delta_layer, sort_delta, sort_delta_value},
        },
        walrecord::NeonWalRecord,
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
    async fn merge_in_between() {
        use crate::repository::Value;
        use bytes::Bytes;

        let harness = TenantHarness::create("merge_iterator_merge_in_between").unwrap();
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
        let test_deltas1 = vec![
            (
                get_key(0),
                Lsn(0x10),
                Value::Image(Bytes::copy_from_slice(b"test")),
            ),
            (
                get_key(5),
                Lsn(0x10),
                Value::Image(Bytes::copy_from_slice(b"test")),
            ),
        ];
        let resident_layer_1 = produce_delta_layer(&tenant, &tline, test_deltas1.clone(), &ctx)
            .await
            .unwrap();
        let test_deltas2 = vec![
            (
                get_key(3),
                Lsn(0x10),
                Value::Image(Bytes::copy_from_slice(b"test")),
            ),
            (
                get_key(4),
                Lsn(0x10),
                Value::Image(Bytes::copy_from_slice(b"test")),
            ),
        ];
        let resident_layer_2 = produce_delta_layer(&tenant, &tline, test_deltas2.clone(), &ctx)
            .await
            .unwrap();
        let mut merge_iter = MergeIterator::create(
            &[
                resident_layer_2.get_as_delta(&ctx).await.unwrap(),
                resident_layer_1.get_as_delta(&ctx).await.unwrap(),
            ],
            &[],
            &ctx,
        );
        let mut expect = Vec::new();
        expect.extend(test_deltas1);
        expect.extend(test_deltas2);
        expect.sort_by(sort_delta);
        assert_merge_iter_equal(&mut merge_iter, &expect).await;
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

    #[tokio::test]
    async fn delta_image_mixed_merge() {
        use crate::repository::Value;
        use bytes::Bytes;

        let harness = TenantHarness::create("merge_iterator_delta_image_mixed_merge").unwrap();
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
        // In this test case, we want to test if the iterator still works correctly with multiple copies
        // of a delta+image at the same LSN, for example, the following sequence a@10=+a, a@10=+a, a@10=ab, a@10=ab.
        // Duplicated deltas/images are possible for old tenants before the full L0 compaction file name fix.
        // An incomplete compaction could produce multiple exactly-the-same delta layers. Force image generation
        // could produce overlapping images. Apart from duplicated deltas/images, in the current storage implementation
        // one key-lsn could have a delta in the delta layer and one image in the image layer. The iterator should
        // correctly process these situations and return everything as-is, and the upper layer of the system
        // will handle duplicated LSNs.
        let test_deltas1 = vec![
            (
                get_key(0),
                Lsn(0x10),
                Value::WalRecord(NeonWalRecord::wal_init()),
            ),
            (
                get_key(0),
                Lsn(0x18),
                Value::WalRecord(NeonWalRecord::wal_append("a")),
            ),
            (
                get_key(5),
                Lsn(0x10),
                Value::WalRecord(NeonWalRecord::wal_init()),
            ),
            (
                get_key(5),
                Lsn(0x18),
                Value::WalRecord(NeonWalRecord::wal_append("b")),
            ),
        ];
        let resident_layer_1 = produce_delta_layer(&tenant, &tline, test_deltas1.clone(), &ctx)
            .await
            .unwrap();
        let mut test_deltas2 = test_deltas1.clone();
        test_deltas2.push((
            get_key(10),
            Lsn(0x20),
            Value::Image(Bytes::copy_from_slice(b"test")),
        ));
        let resident_layer_2 = produce_delta_layer(&tenant, &tline, test_deltas2.clone(), &ctx)
            .await
            .unwrap();
        let test_deltas3 = vec![
            (
                get_key(0),
                Lsn(0x10),
                Value::Image(Bytes::copy_from_slice(b"")),
            ),
            (
                get_key(5),
                Lsn(0x18),
                Value::Image(Bytes::copy_from_slice(b"b")),
            ),
            (
                get_key(15),
                Lsn(0x20),
                Value::Image(Bytes::copy_from_slice(b"test")),
            ),
        ];
        let resident_layer_3 = produce_delta_layer(&tenant, &tline, test_deltas3.clone(), &ctx)
            .await
            .unwrap();
        let mut test_deltas4 = test_deltas3.clone();
        test_deltas4.push((
            get_key(20),
            Lsn(0x20),
            Value::Image(Bytes::copy_from_slice(b"test")),
        ));
        let resident_layer_4 = produce_delta_layer(&tenant, &tline, test_deltas4.clone(), &ctx)
            .await
            .unwrap();
        let mut expect = Vec::new();
        expect.extend(test_deltas1);
        expect.extend(test_deltas2);
        expect.extend(test_deltas3);
        expect.extend(test_deltas4);
        expect.sort_by(sort_delta_value);

        // Test with different layer order for MergeIterator::create to ensure the order
        // is stable.

        let mut merge_iter = MergeIterator::create(
            &[
                resident_layer_4.get_as_delta(&ctx).await.unwrap(),
                resident_layer_1.get_as_delta(&ctx).await.unwrap(),
                resident_layer_3.get_as_delta(&ctx).await.unwrap(),
                resident_layer_2.get_as_delta(&ctx).await.unwrap(),
            ],
            &[],
            &ctx,
        );
        assert_merge_iter_equal(&mut merge_iter, &expect).await;

        let mut merge_iter = MergeIterator::create(
            &[
                resident_layer_1.get_as_delta(&ctx).await.unwrap(),
                resident_layer_4.get_as_delta(&ctx).await.unwrap(),
                resident_layer_3.get_as_delta(&ctx).await.unwrap(),
                resident_layer_2.get_as_delta(&ctx).await.unwrap(),
            ],
            &[],
            &ctx,
        );
        assert_merge_iter_equal(&mut merge_iter, &expect).await;
    }
}
