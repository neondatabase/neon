use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::Subcommand;
use pageserver::tenant::block_io::BlockCursor;
use pageserver::tenant::disk_btree::DiskBtreeReader;
use pageserver::tenant::storage_layer::delta_layer::{BlobRef, Summary};
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
    /// Output layer statistics
    GetStats {
        path: PathBuf,
        tenant: String,
        timeline: String,
        /// The id from list-layer command
        id: usize,
    },
}

// Return (key, value.len) for all keys, sorted by key.
fn read_delta_file(path: impl AsRef<Path>) -> Result<Vec<(Key, usize)>> {
    use pageserver::tenant::blob_io::BlobCursor;
    use pageserver::tenant::block_io::BlockReader;

    let path = path.as_ref();
    virtual_file::init(10);
    page_cache::init(100);
    let file = FileBlockReader::new(VirtualFile::open(path)?);
    let summary_blk = file.read_blk(0)?;
    let actual_summary = Summary::des_prefix(summary_blk.as_ref())?;
    let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
        actual_summary.index_start_blk,
        actual_summary.index_root_blk,
        &file,
    );
    // TODO(chi): dedup w/ `delta_layer.rs` by exposing the API.
    let mut all = vec![];
    tree_reader.visit(
        &[0u8; DELTA_KEY_SIZE],
        VisitDirection::Forwards,
        |key, value_offset| {
            let curr = Key::from_slice(&key[..KEY_SIZE]);
            all.push((curr, BlobRef(value_offset)));
            true
        },
    )?;
    let mut cursor = BlockCursor::new(&file);

    let mut result = vec![];
    for (k, v) in all {
        let value = cursor.read_blob(v.pos())?;
        result.push((k, value.len()));
    }
    // TODO(chi): special handling for last key?
    Ok(result)
}

// We divide the entire i128 keyspace into pre-assigned fixed segments,
// 8MB each. Group keys by segment, and report segment size for each.
//
// 8MB is chosen as the segment size because we're unlikely to make
// s3 partial downloads smaller than 8MB (due to cost). So summarizing
// layer metadata in 8MB segments could be enough to generate good test
// data for write amplification tests.
//
// Note that the segments are fixed, and don't depend on what keyspace
// is actually in use.
fn read_delta_segments(path: impl AsRef<Path>) -> Result<Vec<(i128, usize)>> {
    fn key_to_segment(key: &Key) -> i128 {
        // A page is 8KB. So 1024 pages are 8MB.
        key.to_i128() >> 10
    }

    use itertools::Itertools;
    let delta_metadata = read_delta_file(path)?;
    let group_iter = delta_metadata.iter().group_by(|(k, _)| key_to_segment(k));
    let group_sizes = group_iter.into_iter().map(|(segment, lengths_group)| {
        let sum: usize = lengths_group.map(|(_k, len)| len).sum();
        (segment, sum)
    });
    Ok(group_sizes.collect())
}

fn summarize_delta_file(path: impl AsRef<Path>) -> Result<()> {
    // TODO write in some compressed binary format
    for (segment, size) in read_delta_segments(path)? {
        println!("segment:{} size:{}", segment, size);
    }

    Ok(())
}

pub(crate) fn main(cmd: &LayerCmd) -> Result<()> {
    match cmd {
        LayerCmd::List { path } => {
            for tenant in fs::read_dir(path.join("tenants"))? {
                let tenant = tenant?;
                if !tenant.file_type()?.is_dir() {
                    continue;
                }
                println!("tenant {}", tenant.file_name().to_string_lossy());
                for timeline in fs::read_dir(tenant.path().join("timelines"))? {
                    let timeline = timeline?;
                    if !timeline.file_type()?.is_dir() {
                        continue;
                    }
                    println!("- timeline {}", timeline.file_name().to_string_lossy());
                }
            }
        }
        LayerCmd::ListLayer {
            path,
            tenant,
            timeline,
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
                            for (k, len) in read_delta_file(layer.path())? {
                                println!("key:{} value_len:{}", k, len);
                            }
                        } else {
                            anyhow::bail!("not supported yet :(");
                        }

                        break;
                    }
                    idx += 1;
                }
            }
        }
        LayerCmd::GetStats {
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
                        if layer_file.is_delta {
                            summarize_delta_file(layer.path())?;
                        } else {
                            anyhow::bail!("not supported yet :(");
                        }

                        break;
                    }
                    idx += 1;
                }
            }
        }
    }
    Ok(())
}
