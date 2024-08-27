use std::{future::Future, ops::Range, sync::Arc};

use bytes::Bytes;
use pageserver_api::key::{Key, KEY_SIZE};
use utils::{id::TimelineId, lsn::Lsn, shard::TenantShardId};

use crate::tenant::storage_layer::Layer;
use crate::{config::PageServerConf, context::RequestContext, repository::Value, tenant::Timeline};

use super::layer::S3_UPLOAD_LIMIT;
use super::{
    DeltaLayerWriter, ImageLayerWriter, PersistentLayerDesc, PersistentLayerKey, ResidentLayer,
};

pub(crate) enum SplitWriterResult {
    Produced(ResidentLayer),
    Discarded(PersistentLayerKey),
}

#[cfg(test)]
impl SplitWriterResult {
    fn into_resident_layer(self) -> ResidentLayer {
        match self {
            SplitWriterResult::Produced(layer) => layer,
            SplitWriterResult::Discarded(_) => panic!("unexpected discarded layer"),
        }
    }

    fn into_discarded_layer(self) -> PersistentLayerKey {
        match self {
            SplitWriterResult::Produced(_) => panic!("unexpected produced layer"),
            SplitWriterResult::Discarded(layer) => layer,
        }
    }
}

/// An image writer that takes images and produces multiple image layers.
///
/// The interface does not guarantee atomicity (i.e., if the image layer generation
/// fails, there might be leftover files to be cleaned up)
#[must_use]
pub struct SplitImageLayerWriter {
    inner: ImageLayerWriter,
    target_layer_size: u64,
    generated_layers: Vec<SplitWriterResult>,
    conf: &'static PageServerConf,
    timeline_id: TimelineId,
    tenant_shard_id: TenantShardId,
    lsn: Lsn,
    start_key: Key,
}

impl SplitImageLayerWriter {
    pub async fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        start_key: Key,
        lsn: Lsn,
        target_layer_size: u64,
        ctx: &RequestContext,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            target_layer_size,
            inner: ImageLayerWriter::new(
                conf,
                timeline_id,
                tenant_shard_id,
                &(start_key..Key::MAX),
                lsn,
                ctx,
            )
            .await?,
            generated_layers: Vec::new(),
            conf,
            timeline_id,
            tenant_shard_id,
            lsn,
            start_key,
        })
    }

    pub async fn put_image_with_discard_fn<D, F>(
        &mut self,
        key: Key,
        img: Bytes,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
        discard: D,
    ) -> anyhow::Result<()>
    where
        D: FnOnce(&PersistentLayerKey) -> F,
        F: Future<Output = bool>,
    {
        // The current estimation is an upper bound of the space that the key/image could take
        // because we did not consider compression in this estimation. The resulting image layer
        // could be smaller than the target size.
        let addition_size_estimation = KEY_SIZE as u64 + img.len() as u64;
        if self.inner.num_keys() >= 1
            && self.inner.estimated_size() + addition_size_estimation >= self.target_layer_size
        {
            let next_image_writer = ImageLayerWriter::new(
                self.conf,
                self.timeline_id,
                self.tenant_shard_id,
                &(key..Key::MAX),
                self.lsn,
                ctx,
            )
            .await?;
            let prev_image_writer = std::mem::replace(&mut self.inner, next_image_writer);
            let layer_key = PersistentLayerKey {
                key_range: self.start_key..key,
                lsn_range: PersistentLayerDesc::image_layer_lsn_range(self.lsn),
                is_delta: false,
            };
            self.start_key = key;

            if discard(&layer_key).await {
                drop(prev_image_writer);
                self.generated_layers
                    .push(SplitWriterResult::Discarded(layer_key));
            } else {
                self.generated_layers.push(SplitWriterResult::Produced(
                    prev_image_writer
                        .finish_with_end_key(tline, key, ctx)
                        .await?,
                ));
            }
        }
        self.inner.put_image(key, img, ctx).await
    }

    #[cfg(test)]
    pub async fn put_image(
        &mut self,
        key: Key,
        img: Bytes,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        self.put_image_with_discard_fn(key, img, tline, ctx, |_| async { false })
            .await
    }

    pub(crate) async fn finish_with_discard_fn<D, F>(
        self,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
        end_key: Key,
        discard: D,
    ) -> anyhow::Result<Vec<SplitWriterResult>>
    where
        D: FnOnce(&PersistentLayerKey) -> F,
        F: Future<Output = bool>,
    {
        let Self {
            mut generated_layers,
            inner,
            ..
        } = self;
        if inner.num_keys() == 0 {
            return Ok(generated_layers);
        }
        let layer_key = PersistentLayerKey {
            key_range: self.start_key..end_key,
            lsn_range: PersistentLayerDesc::image_layer_lsn_range(self.lsn),
            is_delta: false,
        };
        if discard(&layer_key).await {
            generated_layers.push(SplitWriterResult::Discarded(layer_key));
        } else {
            generated_layers.push(SplitWriterResult::Produced(
                inner.finish_with_end_key(tline, end_key, ctx).await?,
            ));
        }
        Ok(generated_layers)
    }

    #[cfg(test)]
    pub(crate) async fn finish(
        self,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
        end_key: Key,
    ) -> anyhow::Result<Vec<SplitWriterResult>> {
        self.finish_with_discard_fn(tline, ctx, end_key, |_| async { false })
            .await
    }

    /// When split writer fails, the caller should call this function and handle partially generated layers.
    pub(crate) fn take(self) -> anyhow::Result<(Vec<SplitWriterResult>, ImageLayerWriter)> {
        Ok((self.generated_layers, self.inner))
    }
}

/// A delta writer that takes key-lsn-values and produces multiple delta layers.
///
/// The interface does not guarantee atomicity (i.e., if the delta layer generation fails,
/// there might be leftover files to be cleaned up).
///
/// Note that if updates of a single key exceed the target size limit, all of the updates will be batched
/// into a single file. This behavior might change in the future. For reference, the legacy compaction algorithm
/// will split them into multiple files based on size.
#[must_use]
pub struct SplitDeltaLayerWriter {
    inner: Option<(Key, DeltaLayerWriter)>,
    target_layer_size: u64,
    generated_layers: Vec<SplitWriterResult>,
    conf: &'static PageServerConf,
    timeline_id: TimelineId,
    tenant_shard_id: TenantShardId,
    lsn_range: Range<Lsn>,
    last_key_written: Key,
}

impl SplitDeltaLayerWriter {
    pub async fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        lsn_range: Range<Lsn>,
        target_layer_size: u64,
        #[allow(unused)] ctx: &RequestContext,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            target_layer_size,
            inner: None,
            generated_layers: Vec::new(),
            conf,
            timeline_id,
            tenant_shard_id,
            lsn_range,
            last_key_written: Key::MIN,
        })
    }

    /// Put value into the layer writer. In the case the writer decides to produce a layer, and the discard fn returns true, no layer will be written in the end.
    pub async fn put_value_with_discard_fn<D, F>(
        &mut self,
        key: Key,
        lsn: Lsn,
        val: Value,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
        discard: D,
    ) -> anyhow::Result<()>
    where
        D: FnOnce(&PersistentLayerKey) -> F,
        F: Future<Output = bool>,
    {
        // The current estimation is key size plus LSN size plus value size estimation. This is not an accurate
        // number, and therefore the final layer size could be a little bit larger or smaller than the target.
        //
        // Also, keep all updates of a single key in a single file. TODO: split them using the legacy compaction
        // strategy. https://github.com/neondatabase/neon/issues/8837

        if self.inner.is_none() {
            self.inner = Some((
                key,
                DeltaLayerWriter::new(
                    self.conf,
                    self.timeline_id,
                    self.tenant_shard_id,
                    key,
                    self.lsn_range.clone(),
                    ctx,
                )
                .await?,
            ));
        }
        let (_, inner) = self.inner.as_mut().unwrap();

        let addition_size_estimation = KEY_SIZE as u64 + 8 /* LSN u64 size */ + 80 /* value size estimation */;
        if inner.num_keys() >= 1
            && inner.estimated_size() + addition_size_estimation >= self.target_layer_size
        {
            if key != self.last_key_written {
                let next_delta_writer = DeltaLayerWriter::new(
                    self.conf,
                    self.timeline_id,
                    self.tenant_shard_id,
                    key,
                    self.lsn_range.clone(),
                    ctx,
                )
                .await?;
                let (start_key, prev_delta_writer) =
                    std::mem::replace(&mut self.inner, Some((key, next_delta_writer))).unwrap();
                let layer_key = PersistentLayerKey {
                    key_range: start_key..key,
                    lsn_range: self.lsn_range.clone(),
                    is_delta: true,
                };
                if discard(&layer_key).await {
                    drop(prev_delta_writer);
                    self.generated_layers
                        .push(SplitWriterResult::Discarded(layer_key));
                } else {
                    let (desc, path) = prev_delta_writer.finish(key, ctx).await?;
                    let delta_layer = Layer::finish_creating(self.conf, tline, desc, &path)?;
                    self.generated_layers
                        .push(SplitWriterResult::Produced(delta_layer));
                }
            } else if inner.estimated_size() >= S3_UPLOAD_LIMIT {
                // We have to produce a very large file b/c a key is updated too often.
                anyhow::bail!(
                    "a single key is updated too often: key={}, estimated_size={}, and the layer file cannot be produced",
                    key,
                    inner.estimated_size()
                );
            }
        }
        self.last_key_written = key;
        let (_, inner) = self.inner.as_mut().unwrap();
        inner.put_value(key, lsn, val, ctx).await
    }

    pub async fn put_value(
        &mut self,
        key: Key,
        lsn: Lsn,
        val: Value,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        self.put_value_with_discard_fn(key, lsn, val, tline, ctx, |_| async { false })
            .await
    }

    pub(crate) async fn finish_with_discard_fn<D, F>(
        self,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
        discard: D,
    ) -> anyhow::Result<Vec<SplitWriterResult>>
    where
        D: FnOnce(&PersistentLayerKey) -> F,
        F: Future<Output = bool>,
    {
        let Self {
            mut generated_layers,
            inner,
            ..
        } = self;
        let Some((start_key, inner)) = inner else {
            return Ok(generated_layers);
        };
        if inner.num_keys() == 0 {
            return Ok(generated_layers);
        }
        let end_key = self.last_key_written.next();
        let layer_key = PersistentLayerKey {
            key_range: start_key..end_key,
            lsn_range: self.lsn_range.clone(),
            is_delta: true,
        };
        if discard(&layer_key).await {
            generated_layers.push(SplitWriterResult::Discarded(layer_key));
        } else {
            let (desc, path) = inner.finish(end_key, ctx).await?;
            let delta_layer = Layer::finish_creating(self.conf, tline, desc, &path)?;
            generated_layers.push(SplitWriterResult::Produced(delta_layer));
        }
        Ok(generated_layers)
    }

    #[cfg(test)]
    pub(crate) async fn finish(
        self,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<SplitWriterResult>> {
        self.finish_with_discard_fn(tline, ctx, |_| async { false })
            .await
    }

    /// When split writer fails, the caller should call this function and handle partially generated layers.
    pub(crate) fn take(self) -> anyhow::Result<(Vec<SplitWriterResult>, Option<DeltaLayerWriter>)> {
        Ok((self.generated_layers, self.inner.map(|x| x.1)))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::{RngCore, SeedableRng};

    use crate::{
        tenant::{
            harness::{TenantHarness, TIMELINE_ID},
            storage_layer::AsLayerDesc,
        },
        DEFAULT_PG_VERSION,
    };

    use super::*;

    fn get_key(id: u32) -> Key {
        let mut key = Key::from_hex("000000000033333333444444445500000000").unwrap();
        key.field6 = id;
        key
    }

    fn get_img(id: u32) -> Bytes {
        format!("{id:064}").into()
    }

    fn get_large_img() -> Bytes {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(42);
        let mut data = vec![0; 8192];
        rng.fill_bytes(&mut data);
        data.into()
    }

    #[tokio::test]
    async fn write_one_image() {
        let harness = TenantHarness::create("split_writer_write_one_image")
            .await
            .unwrap();
        let (tenant, ctx) = harness.load().await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        let mut image_writer = SplitImageLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            get_key(0),
            Lsn(0x18),
            4 * 1024 * 1024,
            &ctx,
        )
        .await
        .unwrap();

        let mut delta_writer = SplitDeltaLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            Lsn(0x18)..Lsn(0x20),
            4 * 1024 * 1024,
            &ctx,
        )
        .await
        .unwrap();

        image_writer
            .put_image(get_key(0), get_img(0), &tline, &ctx)
            .await
            .unwrap();
        let layers = image_writer
            .finish(&tline, &ctx, get_key(10))
            .await
            .unwrap();
        assert_eq!(layers.len(), 1);

        delta_writer
            .put_value(
                get_key(0),
                Lsn(0x18),
                Value::Image(get_img(0)),
                &tline,
                &ctx,
            )
            .await
            .unwrap();
        let layers = delta_writer.finish(&tline, &ctx).await.unwrap();
        assert_eq!(layers.len(), 1);
        assert_eq!(
            layers
                .into_iter()
                .next()
                .unwrap()
                .into_resident_layer()
                .layer_desc()
                .key(),
            PersistentLayerKey {
                key_range: get_key(0)..get_key(1),
                lsn_range: Lsn(0x18)..Lsn(0x20),
                is_delta: true
            }
        );
    }

    #[tokio::test]
    async fn write_split() {
        write_split_helper("split_writer_write_split", false).await;
    }

    #[tokio::test]
    async fn write_split_discard() {
        write_split_helper("split_writer_write_split_discard", false).await;
    }

    async fn write_split_helper(harness_name: &'static str, discard: bool) {
        let harness = TenantHarness::create(harness_name).await.unwrap();
        let (tenant, ctx) = harness.load().await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        let mut image_writer = SplitImageLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            get_key(0),
            Lsn(0x18),
            4 * 1024 * 1024,
            &ctx,
        )
        .await
        .unwrap();
        let mut delta_writer = SplitDeltaLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            Lsn(0x18)..Lsn(0x20),
            4 * 1024 * 1024,
            &ctx,
        )
        .await
        .unwrap();
        const N: usize = 2000;
        for i in 0..N {
            let i = i as u32;
            image_writer
                .put_image_with_discard_fn(get_key(i), get_large_img(), &tline, &ctx, |_| async {
                    discard
                })
                .await
                .unwrap();
            delta_writer
                .put_value_with_discard_fn(
                    get_key(i),
                    Lsn(0x20),
                    Value::Image(get_large_img()),
                    &tline,
                    &ctx,
                    |_| async { discard },
                )
                .await
                .unwrap();
        }
        let image_layers = image_writer
            .finish(&tline, &ctx, get_key(N as u32))
            .await
            .unwrap();
        let delta_layers = delta_writer.finish(&tline, &ctx).await.unwrap();
        if discard {
            for layer in image_layers {
                layer.into_discarded_layer();
            }
            for layer in delta_layers {
                layer.into_discarded_layer();
            }
        } else {
            let image_layers = image_layers
                .into_iter()
                .map(|x| x.into_resident_layer())
                .collect_vec();
            let delta_layers = delta_layers
                .into_iter()
                .map(|x| x.into_resident_layer())
                .collect_vec();
            assert_eq!(image_layers.len(), N / 512 + 1);
            assert_eq!(delta_layers.len(), N / 512 + 1);
            assert_eq!(
                delta_layers.first().unwrap().layer_desc().key_range.start,
                get_key(0)
            );
            assert_eq!(
                delta_layers.last().unwrap().layer_desc().key_range.end,
                get_key(N as u32)
            );
            for idx in 0..image_layers.len() {
                assert_ne!(image_layers[idx].layer_desc().key_range.start, Key::MIN);
                assert_ne!(image_layers[idx].layer_desc().key_range.end, Key::MAX);
                assert_ne!(delta_layers[idx].layer_desc().key_range.start, Key::MIN);
                assert_ne!(delta_layers[idx].layer_desc().key_range.end, Key::MAX);
                if idx > 0 {
                    assert_eq!(
                        image_layers[idx - 1].layer_desc().key_range.end,
                        image_layers[idx].layer_desc().key_range.start
                    );
                    assert_eq!(
                        delta_layers[idx - 1].layer_desc().key_range.end,
                        delta_layers[idx].layer_desc().key_range.start
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn write_large_img() {
        let harness = TenantHarness::create("split_writer_write_large_img")
            .await
            .unwrap();
        let (tenant, ctx) = harness.load().await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        let mut image_writer = SplitImageLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            get_key(0),
            Lsn(0x18),
            4 * 1024,
            &ctx,
        )
        .await
        .unwrap();

        let mut delta_writer = SplitDeltaLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            Lsn(0x18)..Lsn(0x20),
            4 * 1024,
            &ctx,
        )
        .await
        .unwrap();

        image_writer
            .put_image(get_key(0), get_img(0), &tline, &ctx)
            .await
            .unwrap();
        image_writer
            .put_image(get_key(1), get_large_img(), &tline, &ctx)
            .await
            .unwrap();
        let layers = image_writer
            .finish(&tline, &ctx, get_key(10))
            .await
            .unwrap();
        assert_eq!(layers.len(), 2);

        delta_writer
            .put_value(
                get_key(0),
                Lsn(0x18),
                Value::Image(get_img(0)),
                &tline,
                &ctx,
            )
            .await
            .unwrap();
        delta_writer
            .put_value(
                get_key(1),
                Lsn(0x1A),
                Value::Image(get_large_img()),
                &tline,
                &ctx,
            )
            .await
            .unwrap();
        let layers = delta_writer.finish(&tline, &ctx).await.unwrap();
        assert_eq!(layers.len(), 2);
        let mut layers_iter = layers.into_iter();
        assert_eq!(
            layers_iter
                .next()
                .unwrap()
                .into_resident_layer()
                .layer_desc()
                .key(),
            PersistentLayerKey {
                key_range: get_key(0)..get_key(1),
                lsn_range: Lsn(0x18)..Lsn(0x20),
                is_delta: true
            }
        );
        assert_eq!(
            layers_iter
                .next()
                .unwrap()
                .into_resident_layer()
                .layer_desc()
                .key(),
            PersistentLayerKey {
                key_range: get_key(1)..get_key(2),
                lsn_range: Lsn(0x18)..Lsn(0x20),
                is_delta: true
            }
        );
    }

    #[tokio::test]
    async fn write_split_single_key() {
        let harness = TenantHarness::create("split_writer_write_split_single_key")
            .await
            .unwrap();
        let (tenant, ctx) = harness.load().await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        const N: usize = 2000;
        let mut delta_writer = SplitDeltaLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            Lsn(0x10)..Lsn(N as u64 * 16 + 0x10),
            4 * 1024 * 1024,
            &ctx,
        )
        .await
        .unwrap();

        for i in 0..N {
            let i = i as u32;
            delta_writer
                .put_value(
                    get_key(0),
                    Lsn(i as u64 * 16 + 0x10),
                    Value::Image(get_large_img()),
                    &tline,
                    &ctx,
                )
                .await
                .unwrap();
        }
        let delta_layers = delta_writer.finish(&tline, &ctx).await.unwrap();
        assert_eq!(delta_layers.len(), 1);
        let delta_layer = delta_layers
            .into_iter()
            .next()
            .unwrap()
            .into_resident_layer();
        assert_eq!(
            delta_layer.layer_desc().key(),
            PersistentLayerKey {
                key_range: get_key(0)..get_key(1),
                lsn_range: Lsn(0x10)..Lsn(N as u64 * 16 + 0x10),
                is_delta: true
            }
        );
    }
}
