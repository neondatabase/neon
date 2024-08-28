use std::path::{Path, PathBuf};

use anyhow::Result;
use camino::{Utf8Path, Utf8PathBuf};
use clap::Subcommand;
use pageserver::config::defaults::DEFAULT_IO_BUFFER_ALIGNMENT;
use pageserver::context::{DownloadBehavior, RequestContext};
use pageserver::task_mgr::TaskKind;
use pageserver::tenant::block_io::BlockCursor;
use pageserver::tenant::disk_btree::DiskBtreeReader;
use pageserver::tenant::storage_layer::delta_layer::{BlobRef, Summary};
use pageserver::tenant::storage_layer::{delta_layer, image_layer};
use pageserver::tenant::storage_layer::{DeltaLayer, ImageLayer};
use pageserver::tenant::{TENANTS_SEGMENT_NAME, TIMELINES_SEGMENT_NAME};
use pageserver::{page_cache, virtual_file};
use pageserver::{
    repository::{Key, KEY_SIZE},
    tenant::{
        block_io::FileBlockReader, disk_btree::VisitDirection,
        storage_layer::delta_layer::DELTA_KEY_SIZE,
    },
    virtual_file::VirtualFile,
};
use std::fs;
use utils::bin_ser::BeSer;
use utils::id::{TenantId, TimelineId};

use crate::layer_map_analyzer::parse_filename;

#[derive(Subcommand)]
pub(crate) enum LayerCmd {
    /// List all tenants and timelines under the pageserver path
    ///
    /// Example: `cargo run --bin pagectl layer list .neon/`
    List { path: PathBuf },
    /// List all layers of a given tenant and timeline
    ///
    /// Example: `cargo run --bin pagectl layer list .neon/`
    ListLayer {
        path: PathBuf,
        tenant: String,
        timeline: String,
    },
    /// Dump all information of a layer file
    DumpLayer {
        path: PathBuf,
        tenant: String,
        timeline: String,
        /// The id from list-layer command
        id: usize,
    },
    RewriteSummary {
        layer_file_path: Utf8PathBuf,
        #[clap(long)]
        new_tenant_id: Option<TenantId>,
        #[clap(long)]
        new_timeline_id: Option<TimelineId>,
    },
}

async fn read_delta_file(path: impl AsRef<Path>, ctx: &RequestContext) -> Result<()> {
    let path = Utf8Path::from_path(path.as_ref()).expect("non-Unicode path");
    virtual_file::init(10, virtual_file::api::IoEngineKind::StdFs, 1);
    page_cache::init(100);
    let file = VirtualFile::open(path, ctx).await?;
    let file_id = page_cache::next_file_id();
    let block_reader = FileBlockReader::new(&file, file_id);
    let summary_blk = block_reader.read_blk(0, ctx).await?;
    let actual_summary = Summary::des_prefix(summary_blk.as_ref())?;
    let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
        actual_summary.index_start_blk,
        actual_summary.index_root_blk,
        &block_reader,
    );
    // TODO(chi): dedup w/ `delta_layer.rs` by exposing the API.
    let mut all = vec![];
    tree_reader
        .visit(
            &[0u8; DELTA_KEY_SIZE],
            VisitDirection::Forwards,
            |key, value_offset| {
                let curr = Key::from_slice(&key[..KEY_SIZE]);
                all.push((curr, BlobRef(value_offset)));
                true
            },
            ctx,
        )
        .await?;
    let cursor = BlockCursor::new_fileblockreader(&block_reader);
    for (k, v) in all {
        let value = cursor.read_blob(v.pos(), ctx).await?;
        println!("key:{} value_len:{}", k, value.len());
    }
    // TODO(chi): special handling for last key?
    Ok(())
}

pub(crate) async fn main(cmd: &LayerCmd) -> Result<()> {
    let ctx = RequestContext::new(TaskKind::DebugTool, DownloadBehavior::Error);
    match cmd {
        LayerCmd::List { path } => {
            for tenant in fs::read_dir(path.join(TENANTS_SEGMENT_NAME))? {
                let tenant = tenant?;
                if !tenant.file_type()?.is_dir() {
                    continue;
                }
                println!("tenant {}", tenant.file_name().to_string_lossy());
                for timeline in fs::read_dir(tenant.path().join(TIMELINES_SEGMENT_NAME))? {
                    let timeline = timeline?;
                    if !timeline.file_type()?.is_dir() {
                        continue;
                    }
                    println!("- timeline {}", timeline.file_name().to_string_lossy());
                }
            }
            Ok(())
        }
        LayerCmd::ListLayer {
            path,
            tenant,
            timeline,
        } => {
            let timeline_path = path
                .join(TENANTS_SEGMENT_NAME)
                .join(tenant)
                .join(TIMELINES_SEGMENT_NAME)
                .join(timeline);
            let mut idx = 0;
            for layer in fs::read_dir(timeline_path)? {
                let layer = layer?;
                if let Some(layer_file) = parse_filename(&layer.file_name().into_string().unwrap())
                {
                    println!(
                        "[{:3}]  key:{}-{}\n       lsn:{}-{}\n       delta:{}",
                        idx,
                        layer_file.key_range.start,
                        layer_file.key_range.end,
                        layer_file.lsn_range.start,
                        layer_file.lsn_range.end,
                        layer_file.is_delta,
                    );
                    idx += 1;
                }
            }
            Ok(())
        }
        LayerCmd::DumpLayer {
            path,
            tenant,
            timeline,
            id,
        } => {
            let timeline_path = path
                .join("tenants")
                .join(tenant)
                .join("timelines")
                .join(timeline);
            let mut idx = 0;
            for layer in fs::read_dir(timeline_path)? {
                let layer = layer?;
                if let Some(layer_file) = parse_filename(&layer.file_name().into_string().unwrap())
                {
                    if *id == idx {
                        // TODO(chi): dedup code
                        println!(
                            "[{:3}]  key:{}-{}\n       lsn:{}-{}\n       delta:{}",
                            idx,
                            layer_file.key_range.start,
                            layer_file.key_range.end,
                            layer_file.lsn_range.start,
                            layer_file.lsn_range.end,
                            layer_file.is_delta,
                        );

                        if layer_file.is_delta {
                            read_delta_file(layer.path(), &ctx).await?;
                        } else {
                            anyhow::bail!("not supported yet :(");
                        }

                        break;
                    }
                    idx += 1;
                }
            }
            Ok(())
        }
        LayerCmd::RewriteSummary {
            layer_file_path,
            new_tenant_id,
            new_timeline_id,
        } => {
            pageserver::virtual_file::init(
                10,
                virtual_file::api::IoEngineKind::StdFs,
                DEFAULT_IO_BUFFER_ALIGNMENT,
            );
            pageserver::page_cache::init(100);

            let ctx = RequestContext::new(TaskKind::DebugTool, DownloadBehavior::Error);

            macro_rules! rewrite_closure {
                ($($summary_ty:tt)*) => {{
                    |summary| $($summary_ty)* {
                        tenant_id: new_tenant_id.unwrap_or(summary.tenant_id),
                        timeline_id: new_timeline_id.unwrap_or(summary.timeline_id),
                        ..summary
                    }
                }};
            }

            let res = ImageLayer::rewrite_summary(
                layer_file_path,
                rewrite_closure!(image_layer::Summary),
                &ctx,
            )
            .await;
            match res {
                Ok(()) => {
                    println!("Successfully rewrote summary of image layer {layer_file_path}");
                    return Ok(());
                }
                Err(image_layer::RewriteSummaryError::MagicMismatch) => (), // fallthrough
                Err(image_layer::RewriteSummaryError::Other(e)) => {
                    return Err(e);
                }
            }

            let res = DeltaLayer::rewrite_summary(
                layer_file_path,
                rewrite_closure!(delta_layer::Summary),
                &ctx,
            )
            .await;
            match res {
                Ok(()) => {
                    println!("Successfully rewrote summary of delta layer {layer_file_path}");
                    return Ok(());
                }
                Err(delta_layer::RewriteSummaryError::MagicMismatch) => (), // fallthrough
                Err(delta_layer::RewriteSummaryError::Other(e)) => {
                    return Err(e);
                }
            }

            anyhow::bail!("not an image or delta layer: {layer_file_path}");
        }
    }
}
