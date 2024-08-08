use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use pageserver_api::key::{Key, KEY_SIZE};
use utils::{id::TimelineId, lsn::Lsn, shard::TenantShardId};

use crate::tenant::storage_layer::Layer;
use crate::{config::PageServerConf, context::RequestContext, repository::Value, tenant::Timeline};

use super::{DeltaLayerWriter, ImageLayerWriter, ResidentLayer};

/// An image writer that takes images and produces multiple image layers. The interface does not
/// guarantee atomicity (i.e., if the image layer generation fails, there might be leftover files
/// to be cleaned up)
#[must_use]
pub struct SplitImageLayerWriter {
    inner: ImageLayerWriter,
    target_layer_size: u64,
    generated_layers: Vec<ResidentLayer>,
    conf: &'static PageServerConf,
    timeline_id: TimelineId,
    tenant_shard_id: TenantShardId,
    lsn: Lsn,
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
        })
    }

    pub async fn put_image(
        &mut self,
        key: Key,
        img: Bytes,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
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
            self.generated_layers.push(
                prev_image_writer
                    .finish_with_end_key(tline, key, ctx)
                    .await?,
            );
        }
        self.inner.put_image(key, img, ctx).await
    }

    pub(crate) async fn finish(
        self,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
        end_key: Key,
    ) -> anyhow::Result<Vec<ResidentLayer>> {
        let Self {
            mut generated_layers,
            inner,
            ..
        } = self;
        generated_layers.push(inner.finish_with_end_key(tline, end_key, ctx).await?);
        Ok(generated_layers)
    }

    /// When split writer fails, the caller should call this function and handle partially generated layers.
    #[allow(dead_code)]
    pub(crate) async fn take(self) -> anyhow::Result<(Vec<ResidentLayer>, ImageLayerWriter)> {
        Ok((self.generated_layers, self.inner))
    }
}

/// A delta writer that takes key-lsn-values and produces multiple delta layers. The interface does not
/// guarantee atomicity (i.e., if the delta layer generation fails, there might be leftover files
/// to be cleaned up).
#[must_use]
pub struct SplitDeltaLayerWriter {
    inner: DeltaLayerWriter,
    target_layer_size: u64,
    generated_layers: Vec<ResidentLayer>,
    conf: &'static PageServerConf,
    timeline_id: TimelineId,
    tenant_shard_id: TenantShardId,
    lsn_range: Range<Lsn>,
}

impl SplitDeltaLayerWriter {
    pub async fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        start_key: Key,
        lsn_range: Range<Lsn>,
        target_layer_size: u64,
        ctx: &RequestContext,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            target_layer_size,
            inner: DeltaLayerWriter::new(
                conf,
                timeline_id,
                tenant_shard_id,
                start_key,
                lsn_range.clone(),
                ctx,
            )
            .await?,
            generated_layers: Vec::new(),
            conf,
            timeline_id,
            tenant_shard_id,
            lsn_range,
        })
    }

    pub async fn put_value(
        &mut self,
        key: Key,
        lsn: Lsn,
        val: Value,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // The current estimation is key size plus LSN size plus value size estimation. This is not an accurate
        // number, and therefore the final layer size could be a little bit larger or smaller than the target.
        let addition_size_estimation = KEY_SIZE as u64 + 8 /* LSN u64 size */ + 80 /* value size estimation */;
        if self.inner.num_keys() >= 1
            && self.inner.estimated_size() + addition_size_estimation >= self.target_layer_size
        {
            let next_delta_writer = DeltaLayerWriter::new(
                self.conf,
                self.timeline_id,
                self.tenant_shard_id,
                key,
                self.lsn_range.clone(),
                ctx,
            )
            .await?;
            let prev_delta_writer = std::mem::replace(&mut self.inner, next_delta_writer);
            let (desc, path) = prev_delta_writer.finish(key, ctx).await?;
            let delta_layer = Layer::finish_creating(self.conf, tline, desc, &path)?;
            self.generated_layers.push(delta_layer);
        }
        self.inner.put_value(key, lsn, val, ctx).await
    }

    pub(crate) async fn finish(
        self,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
        end_key: Key,
    ) -> anyhow::Result<Vec<ResidentLayer>> {
        let Self {
            mut generated_layers,
            inner,
            ..
        } = self;

        let (desc, path) = inner.finish(end_key, ctx).await?;
        let delta_layer = Layer::finish_creating(self.conf, tline, desc, &path)?;
        generated_layers.push(delta_layer);
        Ok(generated_layers)
    }

    /// When split writer fails, the caller should call this function and handle partially generated layers.
    #[allow(dead_code)]
    pub(crate) async fn take(self) -> anyhow::Result<(Vec<ResidentLayer>, DeltaLayerWriter)> {
        Ok((self.generated_layers, self.inner))
    }
}

#[cfg(test)]
mod tests {
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
        vec![0; 8192].into()
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
            get_key(0),
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
        let layers = delta_writer
            .finish(&tline, &ctx, get_key(10))
            .await
            .unwrap();
        assert_eq!(layers.len(), 1);
    }

    #[tokio::test]
    async fn write_split() {
        let harness = TenantHarness::create("split_writer_write_split")
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
            get_key(0),
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
                .put_image(get_key(i), get_large_img(), &tline, &ctx)
                .await
                .unwrap();
            delta_writer
                .put_value(
                    get_key(i),
                    Lsn(0x20),
                    Value::Image(get_large_img()),
                    &tline,
                    &ctx,
                )
                .await
                .unwrap();
        }
        let image_layers = image_writer
            .finish(&tline, &ctx, get_key(N as u32))
            .await
            .unwrap();
        let delta_layers = delta_writer
            .finish(&tline, &ctx, get_key(N as u32))
            .await
            .unwrap();
        assert_eq!(image_layers.len(), N / 512 + 1);
        assert_eq!(delta_layers.len(), N / 512 + 1);
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
            get_key(0),
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
        let layers = delta_writer
            .finish(&tline, &ctx, get_key(10))
            .await
            .unwrap();
        assert_eq!(layers.len(), 2);
    }
}
