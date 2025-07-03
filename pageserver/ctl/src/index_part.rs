use std::str::FromStr;

use anyhow::Context;
use camino::Utf8PathBuf;
use pageserver::tenant::{
    IndexPart,
    layer_map::{LayerMap, SearchResult},
    remote_timeline_client::remote_layer_path,
    storage_layer::{PersistentLayerDesc, ReadableLayerWeak},
};
use pageserver_api::key::Key;
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
    let mut layer_map = LayerMap::default();
    {
        let mut updates = layer_map.batch_update();
        for (key, value) in index_json.layer_metadata.iter() {
            updates.insert_historic(PersistentLayerDesc::from_filename(
                tenant_shard_id,
                timeline_id,
                key.clone(),
                value.file_size,
            ));
        }
    }
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
    }
}
