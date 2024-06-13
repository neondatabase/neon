use std::{
    cmp::Ordering,
    collections::{binary_heap, BinaryHeap},
};

use bytes::Bytes;
use pageserver_api::key::Key;
use utils::lsn::Lsn;

use crate::{context::RequestContext, repository::Value};

use super::image_layer::{ImageLayerInner, ImageLayerIterator};

struct ImageIteratorWrapper<'a, 'ctx> {
    /// The potential next key of the iterator. If the layer is not loaded yet, it will be the start key encoded in the layer file.
    /// Otherwise, it is the next key of the real iterator.
    peek_next_value: Option<(Key, Lsn, Value)>,
    image_layer: &'a ImageLayerInner,
    ctx: &'ctx RequestContext,
    iter: Option<ImageLayerIterator<'a, 'ctx>>,
}

impl<'a, 'ctx> std::cmp::PartialEq for ImageIteratorWrapper<'a, 'ctx> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'a, 'ctx> std::cmp::Eq for ImageIteratorWrapper<'a, 'ctx> {}

impl<'a, 'ctx> std::cmp::PartialOrd for ImageIteratorWrapper<'a, 'ctx> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, 'ctx> std::cmp::Ord for ImageIteratorWrapper<'a, 'ctx> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        let a = self.peek_next_key_lsn();
        let b = other.peek_next_key_lsn();
        match (a, b) {
            (Some((k1, l1)), Some((k2, l2))) => {
                let loaded_1 = if self.is_loaded() { 1 } else { 0 };
                let loaded_2 = if other.is_loaded() { 1 } else { 0 };
                // when key_lsn are the same, the unloaded iter will always appear before the loaded one.
                (k1, l1, loaded_1).cmp(&(k2, l2, loaded_2))
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
        .reverse()
    }
}

impl<'a, 'ctx> ImageIteratorWrapper<'a, 'ctx> {
    pub fn create(image_layer: &'a ImageLayerInner, ctx: &'ctx RequestContext) -> Self {
        Self {
            peek_next_value: Some((
                image_layer.key_range().start,
                image_layer.lsn(),
                Value::Image(Bytes::new()),
            )),
            image_layer,
            ctx,
            iter: None,
        }
    }

    fn peek_next_key_lsn(&self) -> Option<(&Key, Lsn)> {
        let Some((key, lsn, _)) = &self.peek_next_value else {
            return None;
        };
        Some((key, *lsn))
    }

    async fn load(&mut self) -> anyhow::Result<()> {
        assert!(!self.is_loaded());
        let mut iter = self.image_layer.iter(&self.ctx);
        self.peek_next_value = iter.next().await?;
        self.iter = Some(iter);
        Ok(())
    }

    fn is_loaded(&self) -> bool {
        self.iter.is_some()
    }

    async fn next(&mut self) -> anyhow::Result<Option<(Key, Lsn, Value)>> {
        if !self.is_loaded() {
            self.load().await?;
        }
        let result = self.peek_next_value.take();
        let iter = self.iter.as_mut().unwrap();
        self.peek_next_value = iter.next().await?;
        Ok(result)
    }
}

pub struct MergeIterator<'a, 'ctx> {
    heap: BinaryHeap<ImageIteratorWrapper<'a, 'ctx>>,
}

impl<'a, 'ctx> MergeIterator<'a, 'ctx> {
    pub fn create(images: &'a [ImageLayerInner], ctx: &'ctx RequestContext) -> Self {
        let mut heap = BinaryHeap::with_capacity(images.len());
        for image in images {
            heap.push(ImageIteratorWrapper::create(image, ctx));
        }
        Self { heap }
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
