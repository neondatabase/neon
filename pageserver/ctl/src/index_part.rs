use std::str::FromStr;

use anyhow::{Context, Ok};
use camino::Utf8PathBuf;
use pageserver::tenant::{
    IndexPart,
    layer_map::{LayerMap, SearchResult},
    remote_timeline_client::{index::LayerFileMetadata, remote_layer_path},
    storage_layer::{LayerName, LayerVisibilityHint, PersistentLayerDesc, ReadableLayerWeak},
};
use pageserver_api::key::Key;
use serde::Serialize;
use std::collections::BTreeMap;
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
    shard::TenantShardId,
};

#[derive(clap::Subcommand)]
pub(crate) enum IndexPartCmd {
    Dump {
        path: Utf8PathBuf,
    },
    /// Find all layers that need to be searched to construct the given page at the given LSN.
    Search {
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        timeline_id: String,
        #[arg(long)]
        path: Utf8PathBuf,
        #[arg(long)]
        key: String,
        #[arg(long)]
        lsn: String,
    },
    /// List all visible delta and image layers at the latest LSN.
    ListVisibleLayers {
        #[arg(long)]
        path: Utf8PathBuf,
    },
}

fn create_layer_map_from_index_part(
    index_part: &IndexPart,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
) -> LayerMap {
    let mut layer_map = LayerMap::default();
    {
        let mut updates = layer_map.batch_update();
        for (key, value) in index_part.layer_metadata.iter() {
            updates.insert_historic(PersistentLayerDesc::from_filename(
                tenant_shard_id,
                timeline_id,
                key.clone(),
                value.file_size,
            ));
        }
    }
    layer_map
}

async fn search_layers(
    tenant_id: &str,
    timeline_id: &str,
    path: &Utf8PathBuf,
    key: &str,
    lsn: &str,
) -> anyhow::Result<()> {
    let tenant_id = TenantId::from_str(tenant_id).unwrap();
    let tenant_shard_id = TenantShardId::unsharded(tenant_id);
    let timeline_id = TimelineId::from_str(timeline_id).unwrap();
    let index_json = {
        let bytes = tokio::fs::read(path).await?;
        IndexPart::from_json_bytes(&bytes).unwrap()
    };
    let layer_map = create_layer_map_from_index_part(&index_json, tenant_shard_id, timeline_id);
    let key = Key::from_hex(key)?;

    let lsn = Lsn::from_str(lsn).unwrap();
    let mut end_lsn = lsn;
    loop {
        let result = layer_map.search(key, end_lsn);
        match result {
            Some(SearchResult { layer, lsn_floor }) => {
                let disk_layer = match layer {
                    ReadableLayerWeak::PersistentLayer(layer) => layer,
                    ReadableLayerWeak::InMemoryLayer(_) => {
                        anyhow::bail!("unexpected in-memory layer")
                    }
                };

                let metadata = index_json
                    .layer_metadata
                    .get(&disk_layer.layer_name())
                    .unwrap();
                println!(
                    "{}",
                    remote_layer_path(
                        &tenant_id,
                        &timeline_id,
                        metadata.shard,
                        &disk_layer.layer_name(),
                        metadata.generation
                    )
                );
                end_lsn = lsn_floor;
            }
            None => break,
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
struct VisibleLayers {
    pub total_images: u64,
    pub total_image_bytes: u64,
    pub total_deltas: u64,
    pub total_delta_bytes: u64,
    pub layer_metadata: BTreeMap<LayerName, LayerFileMetadata>,
}

impl VisibleLayers {
    pub fn new() -> Self {
        Self {
            layer_metadata: BTreeMap::new(),
            total_images: 0,
            total_image_bytes: 0,
            total_deltas: 0,
            total_delta_bytes: 0,
        }
    }

    pub fn add_layer(&mut self, name: LayerName, layer: LayerFileMetadata) {
        match name {
            LayerName::Image(_) => {
                self.total_images += 1;
                self.total_image_bytes += layer.file_size;
            }
            LayerName::Delta(_) => {
                self.total_deltas += 1;
                self.total_delta_bytes += layer.file_size;
            }
        }
        self.layer_metadata.insert(name, layer);
    }
}

async fn list_visible_layers(path: &Utf8PathBuf) -> anyhow::Result<()> {
    let tenant_id = TenantId::generate();
    let tenant_shard_id = TenantShardId::unsharded(tenant_id);
    let timeline_id = TimelineId::generate();

    let bytes = tokio::fs::read(path).await.context("read file")?;
    let index_part = IndexPart::from_json_bytes(&bytes).context("deserialize")?;
    let layer_map = create_layer_map_from_index_part(&index_part, tenant_shard_id, timeline_id);
    let mut visible_layers = VisibleLayers::new();
    let (layers, _key_space) = layer_map.get_visibility(Vec::new());
    for (layer, visibility) in layers {
        if visibility == LayerVisibilityHint::Visible {
            visible_layers.add_layer(
                layer.layer_name(),
                index_part
                    .layer_metadata
                    .get(&layer.layer_name())
                    .unwrap()
                    .clone(),
            );
        }
    }
    let output = serde_json::to_string_pretty(&visible_layers).context("serialize output")?;
    println!("{output}");

    Ok(())
}

pub(crate) async fn main(cmd: &IndexPartCmd) -> anyhow::Result<()> {
    match cmd {
        IndexPartCmd::Dump { path } => {
            let bytes = tokio::fs::read(path).await.context("read file")?;
            let des: IndexPart = IndexPart::from_json_bytes(&bytes).context("deserialize")?;
            let output = serde_json::to_string_pretty(&des).context("serialize output")?;
            println!("{output}");
            Ok(())
        }
        IndexPartCmd::Search {
            tenant_id,
            timeline_id,
            path,
            key,
            lsn,
        } => search_layers(tenant_id, timeline_id, path, key, lsn).await,
        IndexPartCmd::ListVisibleLayers { path } => list_visible_layers(path).await,
    }
}
