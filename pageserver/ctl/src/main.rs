//! A helper tool to manage pageserver binary files.
//! Accepts a file as an argument, attempts to parse it with all ways possible
//! and prints its interpreted context.
//!
//! Separate, `metadata` subcommand allows to print and update pageserver's metadata file.

mod draw_timeline_dir;
mod index_part;
mod layer_map_analyzer;
mod layers;

use std::{
    str::FromStr,
    time::{Duration, SystemTime},
};

use anyhow::Context;
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
use pageserver_api::{
    key::Key,
    reltag::SlruKind,
    shard::{ShardStripeSize, TenantShardId},
};
use postgres_ffi::ControlFileData;
use remote_storage::{RemotePath, RemoteStorageConfig};
use tokio_util::sync::CancellationToken;
use utils::{
    id::TimelineId,
    logging::{self, LogFormat, TracingErrorLayerEnablement},
    lsn::Lsn,
    project_git_version,
};

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
    /// Debug print a hex key found from logs
    Key(DescribeKeyCommand),
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

/// Roll back the time for the specified prefix using S3 history.
///
/// The command is fairly low level and powerful. Validation is only very light,
/// so it is more powerful, and thus potentially more dangerous.
#[derive(Parser)]
struct TimeTravelRemotePrefixCmd {
    /// A configuration string for the remote_storage configuration.
    ///
    /// Example: `remote_storage = { bucket_name = "aws-storage-bucket-name", bucket_region = "us-east-2" }`
    config_toml_str: String,
    /// remote prefix to time travel recover. For safety reasons, we require it to contain
    /// a timeline or tenant ID in the prefix.
    prefix: String,
    /// Timestamp to travel to. Given in format like `2024-01-20T10:45:45Z`. Assumes UTC and second accuracy.
    travel_to: String,
    /// Timestamp of the start of the operation, must be after any changes we want to roll back and after.
    /// You can use a few seconds before invoking the command. Same format as `travel_to`.
    done_if_after: Option<String>,
}

#[derive(Parser)]
struct AnalyzeLayerMapCmd {
    /// Pageserver data path
    path: Utf8PathBuf,
    /// Max holes
    max_holes: Option<usize>,
}

#[derive(Parser)]
struct DescribeKeyCommand {
    /// Key material in one of the forms:
    /// - hex
    /// - span attributes captured from log
    /// - reltag blocknum
    input: Vec<String>,

    /// The number of shards to calculate what Keys placement would be.
    #[arg(long)]
    shard_count: Option<CustomShardCount>,

    /// The sharding stripe size.
    ///
    /// The default is hardcoded. It makes no sense to provide this without providing
    /// `--shard-count`.
    #[arg(long, requires = "shard-count")]
    stripe_size: Option<u32>,
}

/// Sharded shard count without unsharded count, which the actual ShardCount supports.
#[derive(Clone, Copy)]
struct CustomShardCount(std::num::NonZeroU8);

#[derive(Debug, thiserror::Error)]
enum InvalidShardCount {
    #[error(transparent)]
    ParsingFailed(#[from] std::num::ParseIntError),
    #[error("too few shards")]
    TooFewShards,
}

impl FromStr for CustomShardCount {
    type Err = InvalidShardCount;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner: std::num::NonZeroU8 = s.parse()?;
        if inner.get() < 2 {
            Err(InvalidShardCount::TooFewShards)
        } else {
            Ok(CustomShardCount(inner))
        }
    }
}

impl From<CustomShardCount> for pageserver_api::shard::ShardCount {
    fn from(value: CustomShardCount) -> Self {
        pageserver_api::shard::ShardCount::new(value.0.get())
    }
}

enum KeyMaterial {
    Hex(Key),
    String(SpanAttributesFromLogs),
    Split(
        pageserver_api::reltag::RelTag,
        pageserver_api::reltag::BlockNumber,
    ),
}

impl<S: AsRef<str>> TryFrom<&[S]> for KeyMaterial {
    type Error = anyhow::Error;

    fn try_from(value: &[S]) -> Result<Self, Self::Error> {
        match value {
            [one] => {
                let one = one.as_ref();

                let key = Key::from_hex(one).map(KeyMaterial::Hex);

                let attrs = SpanAttributesFromLogs::from_str(one).map(KeyMaterial::String);

                match (key, attrs) {
                    (Ok(key), _) => Ok(key),
                    (_, Ok(s)) => Ok(s),
                    (Err(e1), Err(e2)) => anyhow::bail!(
                        "failed to parse {one:?} as hex or span attributes:\n- {e1:#}\n- {e2:#}"
                    ),
                }
            }
            [reltag, blocknum] => {
                let (reltag, blocknum) = (reltag.as_ref(), blocknum.as_ref());
                let reltag = reltag.strip_prefix("rel=").unwrap_or(reltag);
                let blocknum = blocknum.strip_prefix("blkno=").unwrap_or(blocknum);
                let reltag = reltag
                    .parse()
                    .with_context(|| format!("parse reltag out of {reltag:?}"))?;
                let blocknum = blocknum
                    .parse()
                    .with_context(|| format!("parse blocknum out of {blocknum:?}"))?;
                Ok(KeyMaterial::Split(reltag, blocknum))
            }
            _ => anyhow::bail!("need 1..2 positionals argument, not {}", value.len()),
        }
    }
}

struct SpanAttributesFromLogs(
    pageserver_api::reltag::RelTag,
    pageserver_api::reltag::BlockNumber,
);

impl std::str::FromStr for SpanAttributesFromLogs {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // accept the span separator but do not require or fail if either is missing
        // "whatever{rel=1663/16389/24615 blkno=1052204 req_lsn=FFFFFFFF/FFFFFFFF}"
        let (_, reltag) = s
            .split_once("rel=")
            .ok_or_else(|| anyhow::anyhow!("cannot find 'rel='"))?;
        let reltag = reltag.split_whitespace().next().unwrap();

        let (_, blocknum) = s
            .split_once("blkno=")
            .ok_or_else(|| anyhow::anyhow!("cannot find 'blkno='"))?;
        let blocknum = blocknum.split_whitespace().next().unwrap();

        let reltag = reltag
            .parse()
            .with_context(|| format!("parse reltag from {reltag:?}"))?;
        let blocknum = blocknum
            .parse()
            .with_context(|| format!("parse blocknum from {blocknum:?}"))?;

        Ok(Self(reltag, blocknum))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init(
        LogFormat::Plain,
        TracingErrorLayerEnablement::EnableWithRustLogFilter,
        logging::Output::Stdout,
    )?;

    logging::replace_panic_hook_with_tracing_panic_hook().forget();

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

            let done_if_after = if let Some(done_if_after) = &cmd.done_if_after {
                humantime::parse_rfc3339(done_if_after).map_err(|_e| {
                    anyhow::anyhow!("Invalid time for done_if_after: '{}'", done_if_after)
                })?
            } else {
                const SAFETY_MARGIN: Duration = Duration::from_secs(3);
                tokio::time::sleep(SAFETY_MARGIN).await;
                // Convert to string representation and back to get rid of sub-second values
                let done_if_after = SystemTime::now();
                tokio::time::sleep(SAFETY_MARGIN).await;
                done_if_after
            };

            let timestamp = strip_subsecond(timestamp);
            let done_if_after = strip_subsecond(done_if_after);

            let Some(prefix) = validate_prefix(&cmd.prefix) else {
                println!("specified prefix '{}' failed validation", cmd.prefix);
                return Ok(());
            };
            let toml_document = toml_edit::Document::from_str(&cmd.config_toml_str)?;
            let toml_item = toml_document
                .get("remote_storage")
                .expect("need remote_storage");
            let config = RemoteStorageConfig::from_toml(toml_item)?.expect("incomplete config");
            let storage = remote_storage::GenericRemoteStorage::from_config(&config);
            let cancel = CancellationToken::new();
            storage
                .unwrap()
                .time_travel_recover(Some(&prefix), timestamp, done_if_after, &cancel)
                .await?;
        }
        Commands::Key(DescribeKeyCommand {
            input,
            shard_count,
            stripe_size,
        }) => {
            let key_material = KeyMaterial::try_from(input.as_slice()).unwrap();

            let (key, kind) = match key_material {
                KeyMaterial::Hex(key) => (key, "hex"),
                KeyMaterial::String(SpanAttributesFromLogs(reltag, blocknum))
                | KeyMaterial::Split(reltag, blocknum) => (
                    pageserver_api::key::rel_block_to_key(reltag, blocknum),
                    "first reltag and blocknum",
                ),
            };

            println!("parsed from {kind}: {key}:");
            println!();
            println!("{key:?}");

            macro_rules! kind_query {
                ($name:ident) => {{
                    let s: &'static str = stringify!($name);
                    let s = s.strip_prefix("is_").unwrap_or(s);
                    let s = s.strip_suffix("_key").unwrap_or(s);

                    #[allow(clippy::needless_borrow)]
                    (s, pageserver_api::key::$name(key))
                }};
            }

            let queries = [
                ("rel_block", pageserver_api::key::is_rel_block_key(&key)),
                kind_query!(is_rel_vm_block_key),
                kind_query!(is_rel_fsm_block_key),
                kind_query!(is_slru_block_key),
                kind_query!(is_inherited_key),
                ("rel_size", pageserver_api::key::is_rel_size_key(&key)),
                (
                    "slru_segment_size",
                    pageserver_api::key::is_slru_segment_size_key(&key),
                ),
            ];

            let recognized_kind = "recognized kind";
            let metadata_key = "metadata key";
            let shard_placement = "shard placement";

            let longest = queries
                .iter()
                .map(|t| t.0)
                .chain([recognized_kind, metadata_key, shard_placement].into_iter())
                .map(|s| s.len())
                .max()
                .unwrap();

            let colon = 1;
            let padding = 1;

            for (name, is) in queries {
                let width = longest - name.len() + colon + padding;
                println!("{}{:width$}{}", name, ":", is);
            }

            #[derive(Debug)]
            #[allow(dead_code)] // debug print is used
            enum RecognizedKeyKind {
                DbDir,
                ControlFile,
                Checkpoint,
                AuxFilesV1,
                SlruDir(Result<SlruKind, u32>),
                AuxFileV2(utils::Hex<[u8; 16]>),
            }

            impl RecognizedKeyKind {
                fn new(key: Key) -> Option<Self> {
                    use RecognizedKeyKind::*;

                    let slru_dir_kind = pageserver_api::key::slru_dir_kind(&key);

                    Some(match key {
                        pageserver_api::key::DBDIR_KEY => DbDir,
                        pageserver_api::key::CONTROLFILE_KEY => ControlFile,
                        pageserver_api::key::CHECKPOINT_KEY => Checkpoint,
                        pageserver_api::key::AUX_FILES_KEY => AuxFilesV1,
                        _ if slru_dir_kind.is_some() => SlruDir(slru_dir_kind.unwrap()),
                        _ if key.is_metadata_key() => {
                            let mut bytes = [0u8; 16];
                            key.extract_metadata_key_to_writer(&mut bytes[..]);
                            AuxFileV2(utils::Hex(bytes))
                        }
                        _ => return None,
                    })
                }
            }

            let width = longest - recognized_kind.len() + colon + padding;
            println!(
                "{}{:width$}{:?}",
                recognized_kind,
                ":",
                RecognizedKeyKind::new(key),
            );

            if let Some(shard_count) = shard_count {
                // seeing the sharding placement might be confusing, so leave it out unless shard
                // count was given.

                let stripe_size = stripe_size.map(ShardStripeSize).unwrap_or_default();
                println!(
                    "# placement with shard_count: {} and stripe_size: {}:",
                    shard_count.0, stripe_size.0
                );
                let width = longest - shard_placement.len() + colon + padding;
                println!(
                    "{}{:width$}{:?}",
                    shard_placement,
                    ":",
                    pageserver_api::shard::describe(&key, shard_count.into(), stripe_size)
                );
            }
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
    // TODO: simplify this part
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

/// Ensures that the given S3 prefix is sufficiently constrained.
/// The command is very risky already and we don't want to expose something
/// that allows usually unintentional and quite catastrophic time travel of
/// an entire bucket, which would be a major catastrophy and away
/// by only one character change (similar to "rm -r /home /username/foobar").
fn validate_prefix(prefix: &str) -> Option<RemotePath> {
    if prefix.is_empty() {
        // Empty prefix means we want to specify the *whole* bucket
        return None;
    }
    let components = prefix.split('/').collect::<Vec<_>>();
    let (last, components) = {
        let last = components.last()?;
        if last.is_empty() {
            (
                components.iter().nth_back(1)?,
                &components[..(components.len() - 1)],
            )
        } else {
            (last, &components[..])
        }
    };
    'valid: {
        if let Ok(_timeline_id) = TimelineId::from_str(last) {
            // Ends in either a tenant or timeline ID
            break 'valid;
        }
        if *last == "timelines" {
            if let Some(before_last) = components.iter().nth_back(1) {
                if let Ok(_tenant_id) = TenantShardId::from_str(before_last) {
                    // Has a valid tenant id
                    break 'valid;
                }
            }
        }

        return None;
    }
    RemotePath::from_string(prefix).ok()
}

fn strip_subsecond(timestamp: SystemTime) -> SystemTime {
    let ts_str = humantime::format_rfc3339_seconds(timestamp).to_string();
    humantime::parse_rfc3339(&ts_str).expect("can't parse just created timestamp")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_prefix() {
        assert_eq!(validate_prefix(""), None);
        assert_eq!(validate_prefix("/"), None);
        #[track_caller]
        fn assert_valid(prefix: &str) {
            let remote_path = RemotePath::from_string(prefix).unwrap();
            assert_eq!(validate_prefix(prefix), Some(remote_path));
        }
        assert_valid("wal/3aa8fcc61f6d357410b7de754b1d9001/641e5342083b2235ee3deb8066819683/");
        // Path is not relative but absolute
        assert_eq!(
            validate_prefix(
                "/wal/3aa8fcc61f6d357410b7de754b1d9001/641e5342083b2235ee3deb8066819683/"
            ),
            None
        );
        assert_valid("wal/3aa8fcc61f6d357410b7de754b1d9001/");
        // Partial tenant IDs should be invalid, S3 will match all tenants with the specific ID prefix
        assert_eq!(validate_prefix("wal/3aa8fcc61f6d357410b7d"), None);
        assert_eq!(validate_prefix("wal"), None);
        assert_eq!(validate_prefix("/wal/"), None);
        assert_valid("pageserver/v1/tenants/3aa8fcc61f6d357410b7de754b1d9001");
        // Partial tenant ID
        assert_eq!(
            validate_prefix("pageserver/v1/tenants/3aa8fcc61f6d357410b"),
            None
        );
        assert_valid("pageserver/v1/tenants/3aa8fcc61f6d357410b7de754b1d9001/timelines");
        assert_valid("pageserver/v1/tenants/3aa8fcc61f6d357410b7de754b1d9001-0004/timelines");
        assert_valid("pageserver/v1/tenants/3aa8fcc61f6d357410b7de754b1d9001/timelines/");
        assert_valid("pageserver/v1/tenants/3aa8fcc61f6d357410b7de754b1d9001/timelines/641e5342083b2235ee3deb8066819683");
        assert_eq!(validate_prefix("pageserver/v1/tenants/"), None);
    }
}
