use std::sync::Arc;

use bytes::Bytes;
use pageserver_api::key::Key;
use utils::lsn::Lsn;

use crate::{
    context::RequestContext,
    repository::Value,
    tenant::storage_layer::{DeltaLayerWriter, ImageLayerWriter},
};

use super::Timeline;

impl Timeline {
    pub(crate) fn force_advance_lsn(self: &Arc<Timeline>, new_lsn: Lsn) {
        self.last_record_lsn.advance(new_lsn);
    }

    pub(crate) async fn force_create_image_layer(
        self: &Arc<Timeline>,
        lsn: Lsn,
        mut images: Vec<(Key, Bytes)>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let last_record_lsn = self.get_last_record_lsn();
        assert!(
            lsn <= last_record_lsn,
            "advance last record lsn before inserting a layer, lsn={lsn}, last_record_lsn={last_record_lsn}"
        );
        images.sort_unstable_by(|(ka, _), (kb, _)| ka.cmp(kb));
        let min_key = *images.first().map(|(k, _)| k).unwrap();
        let max_key = images.last().map(|(k, _)| k).unwrap().next();
        let mut image_layer_writer = ImageLayerWriter::new(
            self.conf,
            self.timeline_id,
            self.tenant_shard_id,
            &(min_key..max_key),
            lsn,
            ctx,
        )
        .await?;
        for (key, img) in images {
            image_layer_writer.put_image(key, img, ctx).await?;
        }
        let image_layer = image_layer_writer.finish(self, ctx).await?;

        {
            let mut guard = self.layers.write().await;
            guard.force_insert_layer(image_layer);
        }

        Ok(())
    }

    #[allow(dead_code)] // will be removed once we have test cases using this function
    pub(crate) async fn force_create_delta_layer(
        self: &Arc<Timeline>,
        mut deltas: Vec<(Key, Lsn, Value)>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let last_record_lsn = self.get_last_record_lsn();
        deltas.sort_unstable_by(|(ka, la, _), (kb, lb, _)| (ka, la).cmp(&(kb, lb)));
        let min_key = *deltas.first().map(|(k, _, _)| k).unwrap();
        let max_key = deltas.last().map(|(k, _, _)| k).unwrap().next();
        let min_lsn = *deltas.iter().map(|(_, lsn, _)| lsn).min().unwrap();
        let max_lsn = Lsn(deltas.iter().map(|(_, lsn, _)| lsn).max().unwrap().0 + 1);
        assert!(
            max_lsn <= last_record_lsn,
            "advance last record lsn before inserting a layer, max_lsn={max_lsn}, last_record_lsn={last_record_lsn}"
        );
        let mut delta_layer_writer = DeltaLayerWriter::new(
            self.conf,
            self.timeline_id,
            self.tenant_shard_id,
            min_key,
            min_lsn..max_lsn,
            ctx,
        )
        .await?;
        for (key, lsn, val) in deltas {
            delta_layer_writer.put_value(key, lsn, val, ctx).await?;
        }
        let delta_layer = delta_layer_writer.finish(max_key, self, ctx).await?;

        {
            let mut guard = self.layers.write().await;
            guard.force_insert_layer(delta_layer);
        }

        Ok(())
    }
}
