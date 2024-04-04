//! A helper tool to manage pageserver binary files.
//! Accepts a file as an argument, attempts to parse it with all ways possible
//! and prints its interpreted context.
//!
//! Separate, `metadata` subcommand allows to print and update pageserver's metadata file.

mod draw_timeline_dir;
mod index_part;
mod layer_map_analyzer;
mod layers;

use std::str::FromStr;

use anyhow::anyhow;
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand};
use index_part::IndexPartCmd;
use layers::LayerCmd;
use pageserver::{
    context::{DownloadBehavior, RequestContext},
    page_cache,
    task_mgr::TaskKind,
    tenant::{dump_layerfile_from_path, metadata::TimelineMetadata},
    virtual_file,
};
use postgres_ffi::ControlFileData;
use remote_storage::{RemotePath, RemoteStorageConfig};
use tokio::fs::read_to_string;
use tokio_util::sync::CancellationToken;
use utils::{lsn::Lsn, project_git_version};

project_git_version!(GIT_VERSION);

#[derive(Parser)]
#[command(
    version = GIT_VERSION,
    about = "Neon Pageserver binutils",
    long_about = "Reads pageserver (and related) binary files management utility"
)]
#[command(propagate_version = true)]
struct CliOpts {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Metadata(MetadataCmd),
    #[command(subcommand)]
    IndexPart(IndexPartCmd),
    PrintLayerFile(PrintLayerFileCmd),
    TimeTravelRemotePrefix(TimeTravelRemotePrefixCmd),
    DrawTimeline {},
    AnalyzeLayerMap(AnalyzeLayerMapCmd),
    #[command(subcommand)]
    Layer(LayerCmd),
}

/// Read and update pageserver metadata file
#[derive(Parser)]
struct MetadataCmd {
    /// Input metadata file path
    metadata_path: Utf8PathBuf,
    /// Replace disk consistent Lsn
    disk_consistent_lsn: Option<Lsn>,
    /// Replace previous record Lsn
    prev_record_lsn: Option<Lsn>,
    /// Replace latest gc cuttoff
    latest_gc_cuttoff: Option<Lsn>,
}

#[derive(Parser)]
struct PrintLayerFileCmd {
    /// Pageserver data path
    path: Utf8PathBuf,
}

#[derive(Parser)]
struct TimeTravelRemotePrefixCmd {
    config_toml_path: Utf8PathBuf,
    // remote prefix to time travel recover
    prefix: String,
    travel_to: String,
    done_if_after: String,
}

#[derive(Parser)]
struct AnalyzeLayerMapCmd {
    /// Pageserver data path
    path: Utf8PathBuf,
    /// Max holes
    max_holes: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = CliOpts::parse();

    match cli.command {
        Commands::Layer(cmd) => {
            layers::main(&cmd).await?;
        }
        Commands::Metadata(cmd) => {
            handle_metadata(&cmd)?;
        }
        Commands::IndexPart(cmd) => {
            index_part::main(&cmd).await?;
        }
        Commands::DrawTimeline {} => {
            draw_timeline_dir::main()?;
        }
        Commands::AnalyzeLayerMap(cmd) => {
            layer_map_analyzer::main(&cmd).await?;
        }
        Commands::PrintLayerFile(cmd) => {
            if let Err(e) = read_pg_control_file(&cmd.path) {
                println!(
                    "Failed to read input file as a pg control one: {e:#}\n\
                    Attempting to read it as layer file"
                );
                print_layerfile(&cmd.path).await?;
            }
        }
        Commands::TimeTravelRemotePrefix(cmd) => {
            let timestamp = humantime::parse_rfc3339(&cmd.travel_to)
                .map_err(|_e| anyhow::anyhow!("Invalid time for travel_to: '{}'", cmd.travel_to))?;

            let done_if_after = humantime::parse_rfc3339(&cmd.done_if_after).map_err(|_e| {
                anyhow::anyhow!("Invalid time for done_if_after: '{}'", cmd.done_if_after)
            })?;

            let toml = read_to_string(&cmd.config_toml_path).await.map_err(|e| {
                anyhow!(
                    "Couldn't read remote storage config toml path at {}: {e:?}",
                    cmd.config_toml_path
                )
            })?;

            let prefix = RemotePath::from_string(&cmd.prefix)?;
            let toml_item = toml_edit::Item::from_str(&toml)?;
            let config = RemoteStorageConfig::from_toml(&toml_item)?.expect("incomplete config");
            let storage = remote_storage::GenericRemoteStorage::from_config(&config);
            let cancel = CancellationToken::new();
            storage
                .unwrap()
                .time_travel_recover(Some(&prefix), timestamp, done_if_after, &cancel)
                .await?;
        }
    };
    Ok(())
}

fn read_pg_control_file(control_file_path: &Utf8Path) -> anyhow::Result<()> {
    let control_file = ControlFileData::decode(&std::fs::read(control_file_path)?)?;
    println!("{control_file:?}");
    let control_file_initdb = Lsn(control_file.checkPoint);
    println!(
        "pg_initdb_lsn: {}, aligned: {}",
        control_file_initdb,
        control_file_initdb.align()
    );
    Ok(())
}

async fn print_layerfile(path: &Utf8Path) -> anyhow::Result<()> {
    // Basic initialization of things that don't change after startup
    virtual_file::init(10, virtual_file::api::IoEngineKind::StdFs);
    page_cache::init(100);
    let ctx = RequestContext::new(TaskKind::DebugTool, DownloadBehavior::Error);
    dump_layerfile_from_path(path, true, &ctx).await
}

fn handle_metadata(
    MetadataCmd {
        metadata_path: path,
        disk_consistent_lsn,
        prev_record_lsn,
        latest_gc_cuttoff,
    }: &MetadataCmd,
) -> Result<(), anyhow::Error> {
    let metadata_bytes = std::fs::read(path)?;
    let mut meta = TimelineMetadata::from_bytes(&metadata_bytes)?;
    println!("Current metadata:\n{meta:?}");
    let mut update_meta = false;
    if let Some(disk_consistent_lsn) = disk_consistent_lsn {
        meta = TimelineMetadata::new(
            *disk_consistent_lsn,
            meta.prev_record_lsn(),
            meta.ancestor_timeline(),
            meta.ancestor_lsn(),
            meta.latest_gc_cutoff_lsn(),
            meta.initdb_lsn(),
            meta.pg_version(),
        );
        update_meta = true;
    }
    if let Some(prev_record_lsn) = prev_record_lsn {
        meta = TimelineMetadata::new(
            meta.disk_consistent_lsn(),
            Some(*prev_record_lsn),
            meta.ancestor_timeline(),
            meta.ancestor_lsn(),
            meta.latest_gc_cutoff_lsn(),
            meta.initdb_lsn(),
            meta.pg_version(),
        );
        update_meta = true;
    }
    if let Some(latest_gc_cuttoff) = latest_gc_cuttoff {
        meta = TimelineMetadata::new(
            meta.disk_consistent_lsn(),
            meta.prev_record_lsn(),
            meta.ancestor_timeline(),
            meta.ancestor_lsn(),
            *latest_gc_cuttoff,
            meta.initdb_lsn(),
            meta.pg_version(),
        );
        update_meta = true;
    }

    if update_meta {
        let metadata_bytes = meta.to_bytes()?;
        std::fs::write(path, metadata_bytes)?;
    }

    Ok(())
}
