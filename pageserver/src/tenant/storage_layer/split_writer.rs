use std::sync::Arc;

use bytes::Bytes;
use pageserver_api::key::{Key, KEY_SIZE};
use utils::{id::TimelineId, lsn::Lsn, shard::TenantShardId};

use crate::{config::PageServerConf, context::RequestContext, tenant::Timeline};

use super::{ImageLayerWriter, ResidentLayer};

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

        let mut writer = SplitImageLayerWriter::new(
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

        writer
            .put_image(get_key(0), get_img(0), &tline, &ctx)
            .await
            .unwrap();
        let layers = writer.finish(&tline, &ctx, get_key(10)).await.unwrap();
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

        let mut writer = SplitImageLayerWriter::new(
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
        const N: usize = 2000;
        for i in 0..N {
            let i = i as u32;
            writer
                .put_image(get_key(i), get_large_img(), &tline, &ctx)
                .await
                .unwrap();
        }
        let layers = writer
            .finish(&tline, &ctx, get_key(N as u32))
            .await
            .unwrap();
        assert_eq!(layers.len(), N / 512 + 1);
        for idx in 0..layers.len() {
            assert_ne!(layers[idx].layer_desc().key_range.start, Key::MIN);
            assert_ne!(layers[idx].layer_desc().key_range.end, Key::MAX);
            if idx > 0 {
                assert_eq!(
                    layers[idx - 1].layer_desc().key_range.end,
                    layers[idx].layer_desc().key_range.start
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

        let mut writer = SplitImageLayerWriter::new(
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

        writer
            .put_image(get_key(0), get_img(0), &tline, &ctx)
            .await
            .unwrap();
        writer
            .put_image(get_key(1), get_large_img(), &tline, &ctx)
            .await
            .unwrap();
        let layers = writer.finish(&tline, &ctx, get_key(10)).await.unwrap();
        assert_eq!(layers.len(), 2);
    }
}
