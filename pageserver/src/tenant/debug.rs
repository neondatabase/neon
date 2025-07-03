use std::{ops::Range, str::FromStr, sync::Arc};

use crate::walredo::RedoAttemptType;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::{Bytes, BytesMut};
use camino::Utf8PathBuf;
use clap::Parser;
use itertools::Itertools;
use pageserver_api::{
    key::Key,
    keyspace::KeySpace,
    shard::{ShardIdentity, ShardStripeSize},
};
use postgres_ffi::PgMajorVersion;
use postgres_ffi::{BLCKSZ, page_is_new, page_set_lsn};
use tracing::Instrument;
use utils::{
    generation::Generation,
    id::{TenantId, TimelineId},
    lsn::Lsn,
    shard::{ShardCount, ShardIndex, ShardNumber},
};
use wal_decoder::models::record::NeonWalRecord;

use crate::{
    context::{DownloadBehavior, RequestContext},
    task_mgr::TaskKind,
    tenant::storage_layer::ValueReconstructState,
    walredo::harness::RedoHarness,
};

use super::{
    WalRedoManager, WalredoManagerId,
    harness::TenantHarness,
    remote_timeline_client::LayerFileMetadata,
    storage_layer::{AsLayerDesc, IoConcurrency, Layer, LayerName, ValuesReconstructState},
};

fn process_page_image(next_record_lsn: Lsn, is_fpw: bool, img_bytes: Bytes) -> Bytes {
    // To match the logic in libs/wal_decoder/src/serialized_batch.rs
    let mut new_image: BytesMut = img_bytes.into();
    if is_fpw && !page_is_new(&new_image) {
        page_set_lsn(&mut new_image, next_record_lsn);
    }
    assert_eq!(new_image.len(), BLCKSZ as usize);
    new_image.freeze()
}

async fn redo_wals(input: &str, key: Key) -> anyhow::Result<()> {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();
    let redo_harness = RedoHarness::new()?;
    let span = redo_harness.span();
    let tenant_conf = pageserver_api::models::TenantConfig {
        ..Default::default()
    };

    let ctx = RequestContext::new(TaskKind::DebugTool, DownloadBehavior::Error);
    let tenant = TenantHarness::create_custom(
        "search_key",
        tenant_conf,
        tenant_id,
        ShardIdentity::unsharded(),
        Generation::new(1),
    )
    .await?
    .do_try_load_with_redo(
        Arc::new(WalRedoManager::Prod(
            WalredoManagerId::next(),
            redo_harness.manager,
        )),
        &ctx,
    )
    .await
    .unwrap();
    let timeline = tenant
        .create_test_timeline(timeline_id, Lsn(0x10), PgMajorVersion::PG16, &ctx)
        .await?;
    let contents = tokio::fs::read_to_string(input)
        .await
        .map_err(|e| anyhow::Error::msg(format!("Failed to read input file {input}: {e}")))
        .unwrap();
    let lines = contents.lines();
    let mut last_wal_lsn: Option<Lsn> = None;
    let state = {
        let mut state = ValueReconstructState::default();
        let mut is_fpw = false;
        let mut is_first_line = true;
        for line in lines {
            if is_first_line {
                is_first_line = false;
                if line.trim() == "FPW" {
                    is_fpw = true;
                }
                continue; // Skip the first line.
            }
            // Each input line is in the "<next_record_lsn>,<base64>" format.
            let (lsn_str, payload_b64) = line
                .split_once(',')
                .expect("Invalid input format: expected '<lsn>,<base64>'");

            // Parse the LSN and decode the payload.
            let lsn = Lsn::from_str(lsn_str.trim()).expect("Invalid LSN format");
            let bytes = Bytes::from(
                STANDARD
                    .decode(payload_b64.trim())
                    .expect("Invalid base64 payload"),
            );

            // The first line is considered the base image, the rest are WAL records.
            if state.img.is_none() {
                state.img = Some((lsn, process_page_image(lsn, is_fpw, bytes)));
            } else {
                let wal_record = NeonWalRecord::Postgres {
                    will_init: false,
                    rec: bytes,
                };
                state.records.push((lsn, wal_record));
                last_wal_lsn.replace(lsn);
            }
        }
        state
    };

    assert!(state.img.is_some(), "No base image found");
    assert!(!state.records.is_empty(), "No WAL records found");
    let result = timeline
        .reconstruct_value(key, last_wal_lsn.unwrap(), state, RedoAttemptType::ReadPage)
        .instrument(span.clone())
        .await?;

    eprintln!("final image: {:?}", STANDARD.encode(result));

    Ok(())
}

async fn search_key(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    dir: String,
    key: Key,
    lsn: Lsn,
) -> anyhow::Result<()> {
    let shard_index = ShardIndex {
        shard_number: ShardNumber(0),
        shard_count: ShardCount(4),
    };

    let redo_harness = RedoHarness::new()?;
    let span = redo_harness.span();
    let tenant_conf = pageserver_api::models::TenantConfig {
        ..Default::default()
    };
    let ctx = RequestContext::new(TaskKind::DebugTool, DownloadBehavior::Error);
    let tenant = TenantHarness::create_custom(
        "search_key",
        tenant_conf,
        tenant_id,
        ShardIdentity::new(
            shard_index.shard_number,
            shard_index.shard_count,
            ShardStripeSize(32768),
        )
        .unwrap(),
        Generation::new(1),
    )
    .await?
    .do_try_load_with_redo(
        Arc::new(WalRedoManager::Prod(
            WalredoManagerId::next(),
            redo_harness.manager,
        )),
        &ctx,
    )
    .await
    .unwrap();

    let timeline = tenant
        .create_test_timeline(timeline_id, Lsn(0x10), PgMajorVersion::PG16, &ctx)
        .await?;

    let mut delta_layers: Vec<Layer> = Vec::new();
    let mut img_layer: Option<Layer> = Option::None;
    let mut dir = tokio::fs::read_dir(dir).await?;
    loop {
        let entry = dir.next_entry().await?;
        if entry.is_none() || !entry.as_ref().unwrap().file_type().await?.is_file() {
            break;
        }
        let path = Utf8PathBuf::from_path_buf(entry.unwrap().path()).unwrap();
        let layer_name = match LayerName::from_str(path.file_name().unwrap()) {
            Ok(name) => name,
            Err(_) => {
                eprintln!("Skipped invalid layer: {path}");
                continue;
            }
        };
        let layer = Layer::for_resident(
            tenant.conf,
            &timeline,
            path.clone(),
            layer_name,
            LayerFileMetadata::new(
                tokio::fs::metadata(path.clone()).await?.len(),
                Generation::new(1),
                shard_index,
            ),
        );
        if layer.layer_desc().is_delta() {
            delta_layers.push(layer.into());
        } else if img_layer.is_none() {
            img_layer = Some(layer.into());
        } else {
            anyhow::bail!("Found multiple image layers");
        }
    }
    // sort delta layers based on the descending order of LSN
    delta_layers.sort_by(|a, b| {
        b.layer_desc()
            .get_lsn_range()
            .start
            .cmp(&a.layer_desc().get_lsn_range().start)
    });

    let mut state = ValuesReconstructState::new(IoConcurrency::Sequential);

    let key_space = KeySpace::single(Range {
        start: key,
        end: key.next(),
    });
    let lsn_range = Range {
        start: img_layer
            .as_ref()
            .map_or(Lsn(0x00), |img| img.layer_desc().image_layer_lsn()),
        end: lsn,
    };
    for delta_layer in delta_layers.iter() {
        delta_layer
            .get_values_reconstruct_data(key_space.clone(), lsn_range.clone(), &mut state, &ctx)
            .await?;
    }

    img_layer
        .as_ref()
        .unwrap()
        .get_values_reconstruct_data(key_space.clone(), lsn_range.clone(), &mut state, &ctx)
        .await?;

    for (_key, result) in std::mem::take(&mut state.keys) {
        let state = result.collect_pending_ios().await?;
        if state.img.is_some() {
            eprintln!(
                "image: {}: {:x?}",
                state.img.as_ref().unwrap().0,
                STANDARD.encode(state.img.as_ref().unwrap().1.clone())
            );
        }
        for delta in state.records.iter() {
            match &delta.1 {
                NeonWalRecord::Postgres { will_init, rec } => {
                    eprintln!(
                        "delta: {}: will_init: {}, {:x?}",
                        delta.0,
                        will_init,
                        STANDARD.encode(rec)
                    );
                }
                _ => {
                    eprintln!("delta: {}: {:x?}", delta.0, delta.1);
                }
            }
        }

        let result = timeline
            .reconstruct_value(key, lsn_range.end, state, RedoAttemptType::ReadPage)
            .instrument(span.clone())
            .await?;
        eprintln!("final image: {lsn} : {result:?}");
    }

    Ok(())
}

/// Redo all WALs against the base image in the input file. Return the base64 encoded final image.
/// Each line in the input file must be in the form "<lsn>,<base64>" where:
///   * `<lsn>` is a PostgreSQL LSN in hexadecimal notation, e.g. `0/16ABCDE`.
///   * `<base64>` is the base64‐encoded page image (first line) or WAL record (subsequent lines).
///
/// The first line provides the base image of a page. The LSN is the LSN of "next record" following
/// the record containing the FPI. For example, if the FPI was extracted from a WAL record occuping
/// [0/1, 0/200) in the WAL stream, the LSN appearing along side the page image here should be 0/200.
///
/// The subsequent lines are WAL records, ordered from the oldest to the newest. The LSN is the
/// record LSN of the WAL record, not the "next record" LSN. For example, if the WAL record here
/// occupies [0/1, 0/200) in the WAL stream, the LSN appearing along side the WAL record here should
/// be 0/1.
#[derive(Parser)]
struct RedoWalsCmd {
    #[clap(long)]
    input: String,
    #[clap(long)]
    key: String,
}

#[tokio::test]
async fn test_redo_wals() -> anyhow::Result<()> {
    let args = std::env::args().collect_vec();
    let pos = args
        .iter()
        .position(|arg| arg == "--")
        .unwrap_or(args.len());
    let slice = &args[pos..args.len()];
    let cmd = match RedoWalsCmd::try_parse_from(slice) {
        Ok(cmd) => cmd,
        Err(err) => {
            eprintln!("{err}");
            return Ok(());
        }
    };

    let key = Key::from_hex(&cmd.key).unwrap();
    redo_wals(&cmd.input, key).await?;

    Ok(())
}

/// Search for a page at the given LSN in all layers of the data_dir.
/// Return the base64-encoded image and all WAL records, as well as the final reconstructed image.
#[derive(Parser)]
struct SearchKeyCmd {
    #[clap(long)]
    tenant_id: String,
    #[clap(long)]
    timeline_id: String,
    #[clap(long)]
    data_dir: String,
    #[clap(long)]
    key: String,
    #[clap(long)]
    lsn: String,
}

#[tokio::test]
async fn test_search_key() -> anyhow::Result<()> {
    let args = std::env::args().collect_vec();
    let pos = args
        .iter()
        .position(|arg| arg == "--")
        .unwrap_or(args.len());
    let slice = &args[pos..args.len()];
    let cmd = match SearchKeyCmd::try_parse_from(slice) {
        Ok(cmd) => cmd,
        Err(err) => {
            eprintln!("{err}");
            return Ok(());
        }
    };

    let tenant_id = TenantId::from_str(&cmd.tenant_id).unwrap();
    let timeline_id = TimelineId::from_str(&cmd.timeline_id).unwrap();
    let key = Key::from_hex(&cmd.key).unwrap();
    let lsn = Lsn::from_str(&cmd.lsn).unwrap();
    search_key(tenant_id, timeline_id, cmd.data_dir, key, lsn).await?;

    Ok(())
}
