//!
//! `neon_local` is an executable that can be used to create a local
//! Neon environment, for testing purposes. The local environment is
//! quite different from the cloud environment with Kubernetes, but it
//! easier to work with locally. The python tests in `test_runner`
//! rely on `neon_local` to set up the environment for each test.
//!
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use compute_api::requests::ComputeClaimsScope;
use compute_api::spec::{ComputeMode, PageserverProtocol};
use control_plane::broker::StorageBroker;
use control_plane::endpoint::{ComputeControlPlane, EndpointTerminateMode};
use control_plane::endpoint::{
    local_pageserver_conf_to_conn_info, tenant_locate_response_to_conn_info,
};
use control_plane::endpoint_storage::{ENDPOINT_STORAGE_DEFAULT_ADDR, EndpointStorage};
use control_plane::local_env;
use control_plane::local_env::{
    EndpointStorageConf, InitForceMode, LocalEnv, NeonBroker, NeonLocalInitConf,
    NeonLocalInitPageserverConf, SafekeeperConf,
};
use control_plane::pageserver::PageServerNode;
use control_plane::safekeeper::SafekeeperNode;
use control_plane::storage_controller::{
    NeonStorageControllerStartArgs, NeonStorageControllerStopArgs, StorageController,
};
use nix::fcntl::{Flock, FlockArg};
use pageserver_api::config::{
    DEFAULT_GRPC_LISTEN_PORT as DEFAULT_PAGESERVER_GRPC_PORT,
    DEFAULT_HTTP_LISTEN_PORT as DEFAULT_PAGESERVER_HTTP_PORT,
    DEFAULT_PG_LISTEN_PORT as DEFAULT_PAGESERVER_PG_PORT,
};
use pageserver_api::controller_api::{
    NodeAvailabilityWrapper, PlacementPolicy, TenantCreateRequest,
};
use pageserver_api::models::{
    ShardParameters, TenantConfigRequest, TimelineCreateRequest, TimelineInfo,
};
use pageserver_api::shard::{DEFAULT_STRIPE_SIZE, ShardCount, ShardStripeSize, TenantShardId};
use postgres_backend::AuthType;
use safekeeper_api::membership::{SafekeeperGeneration, SafekeeperId};
use safekeeper_api::{
    DEFAULT_HTTP_LISTEN_PORT as DEFAULT_SAFEKEEPER_HTTP_PORT,
    DEFAULT_PG_LISTEN_PORT as DEFAULT_SAFEKEEPER_PG_PORT, PgMajorVersion, PgVersionId,
};
use storage_broker::DEFAULT_LISTEN_ADDR as DEFAULT_BROKER_ADDR;
use tokio::task::JoinSet;
use utils::auth::{Claims, Scope};
use utils::id::{NodeId, TenantId, TenantTimelineId, TimelineId};
use utils::lsn::Lsn;
use utils::project_git_version;

// Default id of a safekeeper node, if not specified on the command line.
const DEFAULT_SAFEKEEPER_ID: NodeId = NodeId(1);
const DEFAULT_PAGESERVER_ID: NodeId = NodeId(1);
const DEFAULT_BRANCH_NAME: &str = "main";
project_git_version!(GIT_VERSION);

#[allow(dead_code)]
const DEFAULT_PG_VERSION: PgMajorVersion = PgMajorVersion::PG17;
const DEFAULT_PG_VERSION_NUM: &str = "17";

const DEFAULT_PAGESERVER_CONTROL_PLANE_API: &str = "http://127.0.0.1:1234/upcall/v1/";

/// Neon CLI.
#[derive(clap::Parser)]
#[command(version = GIT_VERSION, name = "Neon CLI")]
struct Cli {
    #[command(subcommand)]
    command: NeonLocalCmd,
}

#[derive(clap::Subcommand)]
enum NeonLocalCmd {
    Init(InitCmdArgs),

    #[command(subcommand)]
    Tenant(TenantCmd),
    #[command(subcommand)]
    Timeline(TimelineCmd),
    #[command(subcommand)]
    Pageserver(PageserverCmd),
    #[command(subcommand)]
    #[clap(alias = "storage_controller")]
    StorageController(StorageControllerCmd),
    #[command(subcommand)]
    #[clap(alias = "storage_broker")]
    StorageBroker(StorageBrokerCmd),
    #[command(subcommand)]
    Safekeeper(SafekeeperCmd),
    #[command(subcommand)]
    EndpointStorage(EndpointStorageCmd),
    #[command(subcommand)]
    Endpoint(EndpointCmd),
    #[command(subcommand)]
    Mappings(MappingsCmd),

    Start(StartCmdArgs),
    Stop(StopCmdArgs),
}

/// Initialize a new Neon repository, preparing configs for services to start with.
#[derive(clap::Args)]
struct InitCmdArgs {
    /// How many pageservers to create (default 1).
    #[clap(long)]
    num_pageservers: Option<u16>,

    #[clap(long)]
    config: Option<PathBuf>,

    /// Force initialization even if the repository is not empty.
    #[clap(long, default_value = "must-not-exist")]
    #[arg(value_parser)]
    force: InitForceMode,
}

/// Start pageserver and safekeepers.
#[derive(clap::Args)]
struct StartCmdArgs {
    #[clap(long = "start-timeout", default_value = "10s")]
    timeout: humantime::Duration,
}

/// Stop pageserver and safekeepers.
#[derive(clap::Args)]
struct StopCmdArgs {
    #[arg(value_enum)]
    #[clap(long, default_value_t = StopMode::Fast)]
    mode: StopMode,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum StopMode {
    Fast,
    Immediate,
}

/// Manage tenants.
#[derive(clap::Subcommand)]
enum TenantCmd {
    List,
    Create(TenantCreateCmdArgs),
    SetDefault(TenantSetDefaultCmdArgs),
    Config(TenantConfigCmdArgs),
    Import(TenantImportCmdArgs),
}

#[derive(clap::Args)]
struct TenantCreateCmdArgs {
    /// Tenant ID, as a 32-byte hexadecimal string.
    #[clap(long = "tenant-id")]
    tenant_id: Option<TenantId>,

    /// Use a specific timeline id when creating a tenant and its initial timeline.
    #[clap(long)]
    timeline_id: Option<TimelineId>,

    #[clap(short = 'c')]
    config: Vec<String>,

    /// Postgres version to use for the initial timeline.
    #[arg(default_value = DEFAULT_PG_VERSION_NUM)]
    #[clap(long)]
    pg_version: PgMajorVersion,

    /// Use this tenant in future CLI commands where tenant_id is needed, but not specified.
    #[clap(long)]
    set_default: bool,

    /// Number of shards in the new tenant.
    #[clap(long)]
    #[arg(default_value_t = 0)]
    shard_count: u8,
    /// Sharding stripe size in pages.
    #[clap(long)]
    shard_stripe_size: Option<u32>,

    /// Placement policy shards in this tenant.
    #[clap(long)]
    #[arg(value_parser = parse_placement_policy)]
    placement_policy: Option<PlacementPolicy>,
}

fn parse_placement_policy(s: &str) -> anyhow::Result<PlacementPolicy> {
    Ok(serde_json::from_str::<PlacementPolicy>(s)?)
}

/// Set a particular tenant as default in future CLI commands where tenant_id is needed, but not
/// specified.
#[derive(clap::Args)]
struct TenantSetDefaultCmdArgs {
    /// Tenant ID, as a 32-byte hexadecimal string.
    #[clap(long = "tenant-id")]
    tenant_id: TenantId,
}

#[derive(clap::Args)]
struct TenantConfigCmdArgs {
    /// Tenant ID, as a 32-byte hexadecimal string.
    #[clap(long = "tenant-id")]
    tenant_id: Option<TenantId>,

    #[clap(short = 'c')]
    config: Vec<String>,
}

/// Import a tenant that is present in remote storage, and create branches for its timelines.
#[derive(clap::Args)]
struct TenantImportCmdArgs {
    /// Tenant ID, as a 32-byte hexadecimal string.
    #[clap(long = "tenant-id")]
    tenant_id: TenantId,
}

/// Manage timelines.
#[derive(clap::Subcommand)]
enum TimelineCmd {
    List(TimelineListCmdArgs),
    Branch(TimelineBranchCmdArgs),
    Create(TimelineCreateCmdArgs),
    Import(TimelineImportCmdArgs),
}

/// List all timelines available to this pageserver.
#[derive(clap::Args)]
struct TimelineListCmdArgs {
    /// Tenant ID, as a 32-byte hexadecimal string.
    #[clap(long = "tenant-id")]
    tenant_shard_id: Option<TenantShardId>,
}

/// Create a new timeline, branching off from another timeline.
#[derive(clap::Args)]
struct TimelineBranchCmdArgs {
    /// Tenant ID, as a 32-byte hexadecimal string.
    #[clap(long = "tenant-id")]
    tenant_id: Option<TenantId>,
    /// New timeline's ID, as a 32-byte hexadecimal string.
    #[clap(long)]
    timeline_id: Option<TimelineId>,
    /// Human-readable alias for the new timeline.
    #[clap(long)]
    branch_name: String,
    /// Use last Lsn of another timeline (and its data) as base when creating the new timeline. The
    /// timeline gets resolved by its branch name.
    #[clap(long)]
    ancestor_branch_name: Option<String>,
    /// When using another timeline as base, use a specific Lsn in it instead of the latest one.
    #[clap(long)]
    ancestor_start_lsn: Option<Lsn>,
}

/// Create a new blank timeline.
#[derive(clap::Args)]
struct TimelineCreateCmdArgs {
    /// Tenant ID, as a 32-byte hexadecimal string.
    #[clap(long = "tenant-id")]
    tenant_id: Option<TenantId>,
    /// New timeline's ID, as a 32-byte hexadecimal string.
    #[clap(long)]
    timeline_id: Option<TimelineId>,
    /// Human-readable alias for the new timeline.
    #[clap(long)]
    branch_name: String,

    /// Postgres version.
    #[arg(default_value = DEFAULT_PG_VERSION_NUM)]
    #[clap(long)]
    pg_version: PgMajorVersion,
}

/// Import a timeline from a basebackup directory.
#[derive(clap::Args)]
struct TimelineImportCmdArgs {
    /// Tenant ID, as a 32-byte hexadecimal string.
    #[clap(long = "tenant-id")]
    tenant_id: Option<TenantId>,
    /// New timeline's ID, as a 32-byte hexadecimal string.
    #[clap(long)]
    timeline_id: TimelineId,
    /// Human-readable alias for the new timeline.
    #[clap(long)]
    branch_name: String,
    /// Basebackup tarfile to import.
    #[clap(long)]
    base_tarfile: PathBuf,
    /// LSN the basebackup starts at.
    #[clap(long)]
    base_lsn: Lsn,
    /// WAL to add after base.
    #[clap(long)]
    wal_tarfile: Option<PathBuf>,
    /// LSN the basebackup ends at.
    #[clap(long)]
    end_lsn: Option<Lsn>,

    /// Postgres version of the basebackup being imported.
    #[arg(default_value = DEFAULT_PG_VERSION_NUM)]
    #[clap(long)]
    pg_version: PgMajorVersion,
}

/// Manage pageservers.
#[derive(clap::Subcommand)]
enum PageserverCmd {
    Status(PageserverStatusCmdArgs),
    Start(PageserverStartCmdArgs),
    Stop(PageserverStopCmdArgs),
    Restart(PageserverRestartCmdArgs),
}

/// Show status of a local pageserver.
#[derive(clap::Args)]
struct PageserverStatusCmdArgs {
    /// Pageserver ID.
    #[clap(long = "id")]
    pageserver_id: Option<NodeId>,
}

/// Start local pageserver.
#[derive(clap::Args)]
struct PageserverStartCmdArgs {
    /// Pageserver ID.
    #[clap(long = "id")]
    pageserver_id: Option<NodeId>,
    /// Timeout until we fail the command.
    #[clap(short = 't', long)]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

/// Stop local pageserver.
#[derive(clap::Args)]
struct PageserverStopCmdArgs {
    /// Pageserver ID.
    #[clap(long = "id")]
    pageserver_id: Option<NodeId>,
    /// If 'immediate', don't flush repository data at shutdown
    #[clap(short = 'm')]
    #[arg(value_enum, default_value = "fast")]
    stop_mode: StopMode,
}

/// Restart local pageserver.
#[derive(clap::Args)]
struct PageserverRestartCmdArgs {
    /// Pageserver ID.
    #[clap(long = "id")]
    pageserver_id: Option<NodeId>,
    /// Timeout until we fail the command.
    #[clap(short = 't', long)]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

/// Manage storage controller.
#[derive(clap::Subcommand)]
enum StorageControllerCmd {
    Start(StorageControllerStartCmdArgs),
    Stop(StorageControllerStopCmdArgs),
}

/// Start storage controller.
#[derive(clap::Args)]
struct StorageControllerStartCmdArgs {
    /// Timeout until we fail the command.
    #[clap(short = 't', long)]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
    /// Identifier used to distinguish storage controller instances.
    #[clap(long)]
    #[arg(default_value_t = 1)]
    instance_id: u8,
    /// Base port for the storage controller instance identified by instance-id (defaults to
    /// pageserver cplane api).
    #[clap(long)]
    base_port: Option<u16>,

    /// Whether the storage controller should handle pageserver-reported local disk loss events.
    #[clap(long)]
    handle_ps_local_disk_loss: Option<bool>,
}

/// Stop storage controller.
#[derive(clap::Args)]
struct StorageControllerStopCmdArgs {
    /// If 'immediate', don't flush repository data at shutdown
    #[clap(short = 'm')]
    #[arg(value_enum, default_value = "fast")]
    stop_mode: StopMode,
    /// Identifier used to distinguish storage controller instances.
    #[clap(long)]
    #[arg(default_value_t = 1)]
    instance_id: u8,
}

/// Manage storage broker.
#[derive(clap::Subcommand)]
enum StorageBrokerCmd {
    Start(StorageBrokerStartCmdArgs),
    Stop(StorageBrokerStopCmdArgs),
}

/// Start broker.
#[derive(clap::Args)]
struct StorageBrokerStartCmdArgs {
    /// Timeout until we fail the command.
    #[clap(short = 't', long, default_value = "10s")]
    start_timeout: humantime::Duration,
}

/// Stop broker.
#[derive(clap::Args)]
struct StorageBrokerStopCmdArgs {
    /// If 'immediate', don't flush repository data on shutdown.
    #[clap(short = 'm')]
    #[arg(value_enum, default_value = "fast")]
    stop_mode: StopMode,
}

/// Manage safekeepers.
#[derive(clap::Subcommand)]
enum SafekeeperCmd {
    Start(SafekeeperStartCmdArgs),
    Stop(SafekeeperStopCmdArgs),
    Restart(SafekeeperRestartCmdArgs),
}

/// Manage object storage.
#[derive(clap::Subcommand)]
enum EndpointStorageCmd {
    Start(EndpointStorageStartCmd),
    Stop(EndpointStorageStopCmd),
}

/// Start object storage.
#[derive(clap::Args)]
struct EndpointStorageStartCmd {
    /// Timeout until we fail the command.
    #[clap(short = 't', long)]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

/// Stop object storage.
#[derive(clap::Args)]
struct EndpointStorageStopCmd {
    /// If 'immediate', don't flush repository data on shutdown.
    #[clap(short = 'm')]
    #[arg(value_enum, default_value = "fast")]
    stop_mode: StopMode,
}

/// Start local safekeeper.
#[derive(clap::Args)]
struct SafekeeperStartCmdArgs {
    /// Safekeeper ID.
    #[arg(default_value_t = NodeId(1))]
    id: NodeId,

    /// Additional safekeeper invocation options, e.g. -e=--http-auth-public-key-path=foo.
    #[clap(short = 'e', long = "safekeeper-extra-opt")]
    extra_opt: Vec<String>,

    /// Timeout until we fail the command.
    #[clap(short = 't', long)]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

/// Stop local safekeeper.
#[derive(clap::Args)]
struct SafekeeperStopCmdArgs {
    /// Safekeeper ID.
    #[arg(default_value_t = NodeId(1))]
    id: NodeId,

    /// If 'immediate', don't flush repository data on shutdown.
    #[arg(value_enum, default_value = "fast")]
    #[clap(short = 'm')]
    stop_mode: StopMode,
}

/// Restart local safekeeper.
#[derive(clap::Args)]
struct SafekeeperRestartCmdArgs {
    /// Safekeeper ID.
    #[arg(default_value_t = NodeId(1))]
    id: NodeId,

    /// If 'immediate', don't flush repository data on shutdown.
    #[arg(value_enum, default_value = "fast")]
    #[clap(short = 'm')]
    stop_mode: StopMode,

    /// Additional safekeeper invocation options, e.g. -e=--http-auth-public-key-path=foo.
    #[clap(short = 'e', long = "safekeeper-extra-opt")]
    extra_opt: Vec<String>,

    /// Timeout until we fail the command.
    #[clap(short = 't', long)]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

/// Manage Postgres instances.
#[derive(clap::Subcommand)]
enum EndpointCmd {
    List(EndpointListCmdArgs),
    Create(EndpointCreateCmdArgs),
    Start(EndpointStartCmdArgs),
    Reconfigure(EndpointReconfigureCmdArgs),
    RefreshConfiguration(EndpointRefreshConfigurationArgs),
    Stop(EndpointStopCmdArgs),
    UpdatePageservers(EndpointUpdatePageserversCmdArgs),
    GenerateJwt(EndpointGenerateJwtCmdArgs),
}

/// List endpoints.
#[derive(clap::Args)]
struct EndpointListCmdArgs {
    /// Tenant ID, as a 32-byte hexadecimal string.
    #[clap(long = "tenant-id")]
    tenant_shard_id: Option<TenantShardId>,
}

/// Create a compute endpoint.
#[derive(clap::Args)]
struct EndpointCreateCmdArgs {
    /// Tenant ID, as a 32-byte hexadecimal string.
    #[clap(long = "tenant-id")]
    tenant_id: Option<TenantId>,
    /// Postgres endpoint ID.
    endpoint_id: Option<String>,
    /// Name of the branch the endpoint will run on.
    #[clap(long)]
    branch_name: Option<String>,
    /// Specify LSN on the timeline to start from. By default, end of the timeline would be used.
    #[clap(long)]
    lsn: Option<Lsn>,
    #[clap(long)]
    pg_port: Option<u16>,
    #[clap(long, alias = "http-port")]
    external_http_port: Option<u16>,
    #[clap(long)]
    internal_http_port: Option<u16>,
    #[clap(long = "pageserver-id")]
    endpoint_pageserver_id: Option<NodeId>,

    /// Don't do basebackup, create endpoint directory with only config files.
    #[clap(long, action = clap::ArgAction::Set, default_value_t = false)]
    config_only: bool,

    /// Postgres version.
    #[arg(default_value = DEFAULT_PG_VERSION_NUM)]
    #[clap(long)]
    pg_version: PgMajorVersion,

    /// Use gRPC to communicate with Pageservers, by generating grpc:// connstrings.
    ///
    /// Specified on creation such that it's retained across reconfiguration and restarts.
    ///
    /// NB: not yet supported by computes.
    #[clap(long)]
    grpc: bool,

    /// If set, the node will be a hot replica on the specified timeline.
    #[clap(long, action = clap::ArgAction::Set, default_value_t = false)]
    hot_standby: bool,
    /// If set, will set up the catalog for neon_superuser.
    #[clap(long)]
    update_catalog: bool,
    /// Allow multiple primary endpoints running on the same branch. Shouldn't be used normally, but
    /// useful for tests.
    #[clap(long)]
    allow_multiple: bool,

    /// Name of the privileged role for the endpoint.
    // Only allow changing it on creation.
    #[clap(long)]
    privileged_role_name: Option<String>,
}

/// Start Postgres. If the endpoint doesn't exist yet, it is created.
#[derive(clap::Args)]
struct EndpointStartCmdArgs {
    /// Postgres endpoint ID.
    endpoint_id: String,
    /// Pageserver ID.
    #[clap(long = "pageserver-id")]
    endpoint_pageserver_id: Option<NodeId>,
    /// Safekeepers membership generation to prefix neon.safekeepers with.
    #[clap(long)]
    safekeepers_generation: Option<u32>,
    /// List of safekeepers endpoint will talk to.
    #[clap(long)]
    safekeepers: Option<String>,
    /// Configure the remote extensions storage proxy gateway URL to request for extensions.
    #[clap(long, alias = "remote-ext-config")]
    remote_ext_base_url: Option<String>,
    /// If set, will create test user `user` and `neondb` database. Requires `update-catalog = true`
    #[clap(long)]
    create_test_user: bool,
    /// Allow multiple primary endpoints running on the same branch. Shouldn't be used normally, but
    /// useful for tests.
    #[clap(long)]
    allow_multiple: bool,
    /// Timeout until we fail the command.
    #[clap(short = 't', long, value_parser= humantime::parse_duration)]
    #[arg(default_value = "90s")]
    start_timeout: Duration,

    /// Download LFC cache from endpoint storage on endpoint startup
    #[clap(long, default_value = "false")]
    autoprewarm: bool,

    /// Upload LFC cache to endpoint storage periodically
    #[clap(long)]
    offload_lfc_interval_seconds: Option<std::num::NonZeroU64>,

    /// Run in development mode, skipping VM-specific operations like process termination
    #[clap(long, action = clap::ArgAction::SetTrue)]
    dev: bool,
}

/// Reconfigure an endpoint.
#[derive(clap::Args)]
struct EndpointReconfigureCmdArgs {
    /// Tenant id. Represented as a hexadecimal string 32 symbols length
    #[clap(long = "tenant-id")]
    tenant_id: Option<TenantId>,
    /// Postgres endpoint ID.
    endpoint_id: String,
    /// Pageserver ID.
    #[clap(long = "pageserver-id")]
    endpoint_pageserver_id: Option<NodeId>,
    #[clap(long)]
    safekeepers: Option<String>,
}

/// Refresh the endpoint's configuration by forcing it reload it's spec
#[derive(clap::Args)]
struct EndpointRefreshConfigurationArgs {
    /// Postgres endpoint id
    endpoint_id: String,
}

/// Stop an endpoint.
#[derive(clap::Args)]
struct EndpointStopCmdArgs {
    /// Postgres endpoint ID.
    endpoint_id: String,
    /// Also delete data directory (now optional, should be default in future).
    #[clap(long)]
    destroy: bool,

    /// Postgres shutdown mode, passed to `pg_ctl -m <mode>`.
    #[clap(long)]
    #[clap(default_value = "fast")]
    mode: EndpointTerminateMode,
}

/// Update the pageservers in the spec file of the compute endpoint
#[derive(clap::Args)]
struct EndpointUpdatePageserversCmdArgs {
    /// Postgres endpoint id
    endpoint_id: String,

    /// Specified pageserver id
    #[clap(short = 'p', long)]
    pageserver_id: Option<NodeId>,
}

/// Generate a JWT for an endpoint.
#[derive(clap::Args)]
struct EndpointGenerateJwtCmdArgs {
    /// Postgres endpoint ID.
    endpoint_id: String,
    /// Scope to generate the JWT with.
    #[clap(short = 's', long, value_parser = ComputeClaimsScope::from_str)]
    scope: Option<ComputeClaimsScope>,
}

/// Manage neon_local branch name mappings.
#[derive(clap::Subcommand)]
enum MappingsCmd {
    Map(MappingsMapCmdArgs),
}

/// Create new mapping which cannot exist already.
#[derive(clap::Args)]
struct MappingsMapCmdArgs {
    /// Tenant ID, as a 32-byte hexadecimal string.
    #[clap(long)]
    tenant_id: TenantId,
    /// Timeline ID, as a 32-byte hexadecimal string.
    #[clap(long)]
    timeline_id: TimelineId,
    /// Branch name to give to the timeline.
    #[clap(long)]
    branch_name: String,
}

///
/// Timelines tree element used as a value in the HashMap.
///
struct TimelineTreeEl {
    /// `TimelineInfo` received from the `pageserver` via the `timeline_list` http API call.
    pub info: TimelineInfo,
    /// Name, recovered from neon config mappings
    pub name: Option<String>,
    /// Holds all direct children of this timeline referenced using `timeline_id`.
    pub children: BTreeSet<TimelineId>,
}

/// A flock-based guard over the neon_local repository directory
struct RepoLock {
    _file: Flock<File>,
}

impl RepoLock {
    fn new() -> Result<Self> {
        let repo_dir = File::open(local_env::base_path())?;
        match Flock::lock(repo_dir, FlockArg::LockExclusive) {
            Ok(f) => Ok(Self { _file: f }),
            Err((_, e)) => Err(e).context("flock error"),
        }
    }
}

// Main entry point for the 'neon_local' CLI utility
//
// This utility helps to manage neon installation. That includes following:
//   * Management of local postgres installations running on top of the
//     pageserver.
//   * Providing CLI api to the pageserver
//   * TODO: export/import to/from usual postgres
fn main() -> Result<()> {
    let cli = Cli::parse();

    // Check for 'neon init' command first.
    let (subcommand_result, _lock) = if let NeonLocalCmd::Init(args) = cli.command {
        (handle_init(&args).map(|env| Some(Cow::Owned(env))), None)
    } else {
        // This tool uses a collection of simple files to store its state, and consequently
        // it is not generally safe to run multiple commands concurrently.  Rather than expect
        // all callers to know this, use a lock file to protect against concurrent execution.
        let _repo_lock = RepoLock::new().unwrap();

        // all other commands need an existing config
        let env = LocalEnv::load_config(&local_env::base_path()).context("Error loading config")?;
        let original_env = env.clone();
        let env = Box::leak(Box::new(env));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let subcommand_result = match cli.command {
            NeonLocalCmd::Init(_) => unreachable!("init was handled earlier already"),
            NeonLocalCmd::Start(args) => rt.block_on(handle_start_all(&args, env)),
            NeonLocalCmd::Stop(args) => rt.block_on(handle_stop_all(&args, env)),
            NeonLocalCmd::Tenant(subcmd) => rt.block_on(handle_tenant(&subcmd, env)),
            NeonLocalCmd::Timeline(subcmd) => rt.block_on(handle_timeline(&subcmd, env)),
            NeonLocalCmd::Pageserver(subcmd) => rt.block_on(handle_pageserver(&subcmd, env)),
            NeonLocalCmd::StorageController(subcmd) => {
                rt.block_on(handle_storage_controller(&subcmd, env))
            }
            NeonLocalCmd::StorageBroker(subcmd) => rt.block_on(handle_storage_broker(&subcmd, env)),
            NeonLocalCmd::Safekeeper(subcmd) => rt.block_on(handle_safekeeper(&subcmd, env)),
            NeonLocalCmd::EndpointStorage(subcmd) => {
                rt.block_on(handle_endpoint_storage(&subcmd, env))
            }
            NeonLocalCmd::Endpoint(subcmd) => rt.block_on(handle_endpoint(&subcmd, env)),
            NeonLocalCmd::Mappings(subcmd) => handle_mappings(&subcmd, env),
        };

        let subcommand_result = if &original_env != env {
            subcommand_result.map(|()| Some(Cow::Borrowed(env)))
        } else {
            subcommand_result.map(|()| None)
        };
        (subcommand_result, Some(_repo_lock))
    };

    match subcommand_result {
        Ok(Some(updated_env)) => updated_env.persist_config()?,
        Ok(None) => (),
        Err(e) => {
            eprintln!("command failed: {e:?}");
            exit(1);
        }
    }
    Ok(())
}

///
/// Prints timelines list as a tree-like structure.
///
fn print_timelines_tree(
    timelines: Vec<TimelineInfo>,
    mut timeline_name_mappings: HashMap<TenantTimelineId, String>,
) -> Result<()> {
    let mut timelines_hash = timelines
        .iter()
        .map(|t| {
            (
                t.timeline_id,
                TimelineTreeEl {
                    info: t.clone(),
                    children: BTreeSet::new(),
                    name: timeline_name_mappings
                        .remove(&TenantTimelineId::new(t.tenant_id.tenant_id, t.timeline_id)),
                },
            )
        })
        .collect::<HashMap<_, _>>();

    // Memorize all direct children of each timeline.
    for timeline in timelines.iter() {
        if let Some(ancestor_timeline_id) = timeline.ancestor_timeline_id {
            timelines_hash
                .get_mut(&ancestor_timeline_id)
                .context("missing timeline info in the HashMap")?
                .children
                .insert(timeline.timeline_id);
        }
    }

    for timeline in timelines_hash.values() {
        // Start with root local timelines (no ancestors) first.
        if timeline.info.ancestor_timeline_id.is_none() {
            print_timeline(0, &Vec::from([true]), timeline, &timelines_hash)?;
        }
    }

    Ok(())
}

///
/// Recursively prints timeline info with all its children.
///
fn print_timeline(
    nesting_level: usize,
    is_last: &[bool],
    timeline: &TimelineTreeEl,
    timelines: &HashMap<TimelineId, TimelineTreeEl>,
) -> Result<()> {
    if nesting_level > 0 {
        let ancestor_lsn = match timeline.info.ancestor_lsn {
            Some(lsn) => lsn.to_string(),
            None => "Unknown Lsn".to_string(),
        };

        let mut br_sym = "┣━";

        // Draw each nesting padding with proper style
        // depending on whether its timeline ended or not.
        if nesting_level > 1 {
            for l in &is_last[1..is_last.len() - 1] {
                if *l {
                    print!("   ");
                } else {
                    print!("┃  ");
                }
            }
        }

        // We are the last in this sub-timeline
        if *is_last.last().unwrap() {
            br_sym = "┗━";
        }

        print!("{br_sym} @{ancestor_lsn}: ");
    }

    // Finally print a timeline id and name with new line
    println!(
        "{} [{}]",
        timeline.name.as_deref().unwrap_or("_no_name_"),
        timeline.info.timeline_id
    );

    let len = timeline.children.len();
    let mut i: usize = 0;
    let mut is_last_new = Vec::from(is_last);
    is_last_new.push(false);

    for child in &timeline.children {
        i += 1;

        // Mark that the last padding is the end of the timeline
        if i == len {
            if let Some(last) = is_last_new.last_mut() {
                *last = true;
            }
        }

        print_timeline(
            nesting_level + 1,
            &is_last_new,
            timelines
                .get(child)
                .context("missing timeline info in the HashMap")?,
            timelines,
        )?;
    }

    Ok(())
}

/// Helper function to get tenant id from an optional --tenant_id option or from the config file
fn get_tenant_id(
    tenant_id_arg: Option<TenantId>,
    env: &local_env::LocalEnv,
) -> anyhow::Result<TenantId> {
    if let Some(tenant_id_from_arguments) = tenant_id_arg {
        Ok(tenant_id_from_arguments)
    } else if let Some(default_id) = env.default_tenant_id {
        Ok(default_id)
    } else {
        anyhow::bail!("No tenant id. Use --tenant-id, or set a default tenant");
    }
}

/// Helper function to get tenant-shard ID from an optional --tenant_id option or from the config file,
/// for commands that accept a shard suffix
fn get_tenant_shard_id(
    tenant_shard_id_arg: Option<TenantShardId>,
    env: &local_env::LocalEnv,
) -> anyhow::Result<TenantShardId> {
    if let Some(tenant_id_from_arguments) = tenant_shard_id_arg {
        Ok(tenant_id_from_arguments)
    } else if let Some(default_id) = env.default_tenant_id {
        Ok(TenantShardId::unsharded(default_id))
    } else {
        anyhow::bail!("No tenant shard id. Use --tenant-id, or set a default tenant");
    }
}

fn handle_init(args: &InitCmdArgs) -> anyhow::Result<LocalEnv> {
    // Create the in-memory `LocalEnv` that we'd normally load from disk in `load_config`.
    let init_conf: NeonLocalInitConf = if let Some(config_path) = &args.config {
        // User (likely the Python test suite) provided a description of the environment.
        if args.num_pageservers.is_some() {
            bail!(
                "Cannot specify both --num-pageservers and --config, use key `pageservers` in the --config file instead"
            );
        }
        // load and parse the file
        let contents = std::fs::read_to_string(config_path).with_context(|| {
            format!(
                "Could not read configuration file '{}'",
                config_path.display()
            )
        })?;
        toml_edit::de::from_str(&contents)?
    } else {
        // User (likely interactive) did not provide a description of the environment, give them the default
        NeonLocalInitConf {
            control_plane_api: Some(DEFAULT_PAGESERVER_CONTROL_PLANE_API.parse().unwrap()),
            broker: NeonBroker {
                listen_addr: Some(DEFAULT_BROKER_ADDR.parse().unwrap()),
                listen_https_addr: None,
            },
            safekeepers: vec![SafekeeperConf {
                id: DEFAULT_SAFEKEEPER_ID,
                pg_port: DEFAULT_SAFEKEEPER_PG_PORT,
                http_port: DEFAULT_SAFEKEEPER_HTTP_PORT,
                ..Default::default()
            }],
            pageservers: (0..args.num_pageservers.unwrap_or(1))
                .map(|i| {
                    let pageserver_id = NodeId(DEFAULT_PAGESERVER_ID.0 + i as u64);
                    let pg_port = DEFAULT_PAGESERVER_PG_PORT + i;
                    let http_port = DEFAULT_PAGESERVER_HTTP_PORT + i;
                    let grpc_port = DEFAULT_PAGESERVER_GRPC_PORT + i;
                    NeonLocalInitPageserverConf {
                        id: pageserver_id,
                        listen_pg_addr: format!("127.0.0.1:{pg_port}"),
                        listen_http_addr: format!("127.0.0.1:{http_port}"),
                        listen_https_addr: None,
                        listen_grpc_addr: Some(format!("127.0.0.1:{grpc_port}")),
                        pg_auth_type: AuthType::Trust,
                        http_auth_type: AuthType::Trust,
                        grpc_auth_type: AuthType::Trust,
                        other: Default::default(),
                        // Typical developer machines use disks with slow fsync, and we don't care
                        // about data integrity: disable disk syncs.
                        no_sync: true,
                    }
                })
                .collect(),
            endpoint_storage: EndpointStorageConf {
                listen_addr: ENDPOINT_STORAGE_DEFAULT_ADDR,
            },
            pg_distrib_dir: None,
            neon_distrib_dir: None,
            default_tenant_id: TenantId::from_array(std::array::from_fn(|_| 0)),
            storage_controller: None,
            control_plane_hooks_api: None,
            generate_local_ssl_certs: false,
        }
    };

    LocalEnv::init(init_conf, &args.force)
        .context("materialize initial neon_local environment on disk")?;
    Ok(LocalEnv::load_config(&local_env::base_path())
        .expect("freshly written config should be loadable"))
}

/// The default pageserver is the one where CLI tenant/timeline operations are sent by default.
/// For typical interactive use, one would just run with a single pageserver.  Scenarios with
/// tenant/timeline placement across multiple pageservers are managed by python test code rather
/// than this CLI.
fn get_default_pageserver(env: &local_env::LocalEnv) -> PageServerNode {
    let ps_conf = env
        .pageservers
        .first()
        .expect("Config is validated to contain at least one pageserver");
    PageServerNode::from_env(env, ps_conf)
}

async fn handle_tenant(subcmd: &TenantCmd, env: &mut local_env::LocalEnv) -> anyhow::Result<()> {
    let pageserver = get_default_pageserver(env);
    match subcmd {
        TenantCmd::List => {
            for t in pageserver.tenant_list().await? {
                println!("{} {:?}", t.id, t.state);
            }
        }
        TenantCmd::Import(args) => {
            let tenant_id = args.tenant_id;

            let storage_controller = StorageController::from_env(env);
            let create_response = storage_controller.tenant_import(tenant_id).await?;

            let shard_zero = create_response
                .shards
                .first()
                .expect("Import response omitted shards");

            let attached_pageserver_id = shard_zero.node_id;
            let pageserver =
                PageServerNode::from_env(env, env.get_pageserver_conf(attached_pageserver_id)?);

            println!(
                "Imported tenant {tenant_id}, attached to pageserver {attached_pageserver_id}"
            );

            let timelines = pageserver
                .http_client
                .list_timelines(shard_zero.shard_id)
                .await?;

            // Pick a 'main' timeline that has no ancestors, the rest will get arbitrary names
            let main_timeline = timelines
                .iter()
                .find(|t| t.ancestor_timeline_id.is_none())
                .expect("No timelines found")
                .timeline_id;

            let mut branch_i = 0;
            for timeline in timelines.iter() {
                let branch_name = if timeline.timeline_id == main_timeline {
                    "main".to_string()
                } else {
                    branch_i += 1;
                    format!("branch_{branch_i}")
                };

                println!(
                    "Importing timeline {tenant_id}/{} as branch {branch_name}",
                    timeline.timeline_id
                );

                env.register_branch_mapping(branch_name, tenant_id, timeline.timeline_id)?;
            }
        }
        TenantCmd::Create(args) => {
            let tenant_conf: HashMap<_, _> =
                args.config.iter().flat_map(|c| c.split_once(':')).collect();

            let tenant_conf = PageServerNode::parse_config(tenant_conf)?;

            // If tenant ID was not specified, generate one
            let tenant_id = args.tenant_id.unwrap_or_else(TenantId::generate);

            // We must register the tenant with the storage controller, so
            // that when the pageserver restarts, it will be re-attached.
            let storage_controller = StorageController::from_env(env);
            storage_controller
                .tenant_create(TenantCreateRequest {
                    // Note that ::unsharded here isn't actually because the tenant is unsharded, its because the
                    // storage controller expects a shard-naive tenant_id in this attribute, and the TenantCreateRequest
                    // type is used both in the storage controller (for creating tenants) and in the pageserver (for
                    // creating shards)
                    new_tenant_id: TenantShardId::unsharded(tenant_id),
                    generation: None,
                    shard_parameters: ShardParameters {
                        count: ShardCount::new(args.shard_count),
                        stripe_size: args
                            .shard_stripe_size
                            .map(ShardStripeSize)
                            .unwrap_or(DEFAULT_STRIPE_SIZE),
                    },
                    placement_policy: args.placement_policy.clone(),
                    config: tenant_conf,
                })
                .await?;
            println!("tenant {tenant_id} successfully created on the pageserver");

            // Create an initial timeline for the new tenant
            let new_timeline_id = args.timeline_id.unwrap_or(TimelineId::generate());

            // FIXME: passing None for ancestor_start_lsn is not kosher in a sharded world: we can't have
            // different shards picking different start lsns.  Maybe we have to teach storage controller
            // to let shard 0 branch first and then propagate the chosen LSN to other shards.
            storage_controller
                .tenant_timeline_create(
                    tenant_id,
                    TimelineCreateRequest {
                        new_timeline_id,
                        mode: pageserver_api::models::TimelineCreateRequestMode::Bootstrap {
                            existing_initdb_timeline_id: None,
                            pg_version: Some(args.pg_version),
                        },
                    },
                )
                .await?;

            env.register_branch_mapping(
                DEFAULT_BRANCH_NAME.to_string(),
                tenant_id,
                new_timeline_id,
            )?;

            println!("Created an initial timeline '{new_timeline_id}' for tenant: {tenant_id}",);

            if args.set_default {
                println!("Setting tenant {tenant_id} as a default one");
                env.default_tenant_id = Some(tenant_id);
            }
        }
        TenantCmd::SetDefault(args) => {
            println!("Setting tenant {} as a default one", args.tenant_id);
            env.default_tenant_id = Some(args.tenant_id);
        }
        TenantCmd::Config(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let tenant_conf: HashMap<_, _> =
                args.config.iter().flat_map(|c| c.split_once(':')).collect();
            let config = PageServerNode::parse_config(tenant_conf)?;

            let req = TenantConfigRequest { tenant_id, config };

            let storage_controller = StorageController::from_env(env);
            storage_controller
                .set_tenant_config(&req)
                .await
                .with_context(|| format!("Tenant config failed for tenant with id {tenant_id}"))?;
            println!("tenant {tenant_id} successfully configured via storcon");
        }
    }
    Ok(())
}

async fn handle_timeline(cmd: &TimelineCmd, env: &mut local_env::LocalEnv) -> Result<()> {
    let pageserver = get_default_pageserver(env);

    match cmd {
        TimelineCmd::List(args) => {
            // TODO(sharding): this command shouldn't have to specify a shard ID: we should ask the storage controller
            // where shard 0 is attached, and query there.
            let tenant_shard_id = get_tenant_shard_id(args.tenant_shard_id, env)?;
            let timelines = pageserver.timeline_list(&tenant_shard_id).await?;
            print_timelines_tree(timelines, env.timeline_name_mappings())?;
        }
        TimelineCmd::Create(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let new_branch_name = &args.branch_name;
            let new_timeline_id_opt = args.timeline_id;
            let new_timeline_id = new_timeline_id_opt.unwrap_or(TimelineId::generate());

            let storage_controller = StorageController::from_env(env);
            let create_req = TimelineCreateRequest {
                new_timeline_id,
                mode: pageserver_api::models::TimelineCreateRequestMode::Bootstrap {
                    existing_initdb_timeline_id: None,
                    pg_version: Some(args.pg_version),
                },
            };
            let timeline_info = storage_controller
                .tenant_timeline_create(tenant_id, create_req)
                .await?;

            let last_record_lsn = timeline_info.last_record_lsn;
            env.register_branch_mapping(new_branch_name.to_string(), tenant_id, new_timeline_id)?;

            println!(
                "Created timeline '{}' at Lsn {last_record_lsn} for tenant: {tenant_id}",
                timeline_info.timeline_id
            );
        }
        // TODO: rename to import-basebackup-plus-wal
        TimelineCmd::Import(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let timeline_id = args.timeline_id;
            let branch_name = &args.branch_name;

            // Parse base inputs
            let base = (args.base_lsn, args.base_tarfile.clone());

            // Parse pg_wal inputs
            let wal_tarfile = args.wal_tarfile.clone();
            let end_lsn = args.end_lsn;
            // TODO validate both or none are provided
            let pg_wal = end_lsn.zip(wal_tarfile);

            println!("Importing timeline into pageserver ...");
            pageserver
                .timeline_import(tenant_id, timeline_id, base, pg_wal, args.pg_version)
                .await?;
            if env.storage_controller.timelines_onto_safekeepers {
                println!("Creating timeline on safekeeper ...");
                let timeline_info = pageserver
                    .timeline_info(
                        TenantShardId::unsharded(tenant_id),
                        timeline_id,
                        pageserver_client::mgmt_api::ForceAwaitLogicalSize::No,
                    )
                    .await?;
                let default_sk = SafekeeperNode::from_env(env, env.safekeepers.first().unwrap());
                let default_host = default_sk
                    .conf
                    .listen_addr
                    .clone()
                    .unwrap_or_else(|| "localhost".to_string());
                let mconf = safekeeper_api::membership::Configuration {
                    generation: SafekeeperGeneration::new(1),
                    members: safekeeper_api::membership::MemberSet {
                        m: vec![SafekeeperId {
                            host: default_host,
                            id: default_sk.conf.id,
                            pg_port: default_sk.conf.pg_port,
                        }],
                    },
                    new_members: None,
                };
                let pg_version = PgVersionId::from(args.pg_version);
                let req = safekeeper_api::models::TimelineCreateRequest {
                    tenant_id,
                    timeline_id,
                    mconf,
                    pg_version,
                    system_id: None,
                    wal_seg_size: None,
                    start_lsn: timeline_info.last_record_lsn,
                    commit_lsn: None,
                };
                default_sk.create_timeline(&req).await?;
            }
            env.register_branch_mapping(branch_name.to_string(), tenant_id, timeline_id)?;
            println!("Done");
        }
        TimelineCmd::Branch(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let new_timeline_id = args.timeline_id.unwrap_or(TimelineId::generate());
            let new_branch_name = &args.branch_name;
            let ancestor_branch_name = args
                .ancestor_branch_name
                .clone()
                .unwrap_or(DEFAULT_BRANCH_NAME.to_owned());
            let ancestor_timeline_id = env
                .get_branch_timeline_id(&ancestor_branch_name, tenant_id)
                .ok_or_else(|| {
                    anyhow!("Found no timeline id for branch name '{ancestor_branch_name}'")
                })?;

            let start_lsn = args.ancestor_start_lsn;
            let storage_controller = StorageController::from_env(env);
            let create_req = TimelineCreateRequest {
                new_timeline_id,
                mode: pageserver_api::models::TimelineCreateRequestMode::Branch {
                    ancestor_timeline_id,
                    ancestor_start_lsn: start_lsn,
                    read_only: false,
                    pg_version: None,
                },
            };
            let timeline_info = storage_controller
                .tenant_timeline_create(tenant_id, create_req)
                .await?;

            let last_record_lsn = timeline_info.last_record_lsn;

            env.register_branch_mapping(new_branch_name.to_string(), tenant_id, new_timeline_id)?;

            println!(
                "Created timeline '{}' at Lsn {last_record_lsn} for tenant: {tenant_id}. Ancestor timeline: '{ancestor_branch_name}'",
                timeline_info.timeline_id
            );
        }
    }

    Ok(())
}

async fn handle_endpoint(subcmd: &EndpointCmd, env: &local_env::LocalEnv) -> Result<()> {
    let mut cplane = ComputeControlPlane::load(env.clone())?;

    match subcmd {
        EndpointCmd::List(args) => {
            // TODO(sharding): this command shouldn't have to specify a shard ID: we should ask the storage controller
            // where shard 0 is attached, and query there.
            let tenant_shard_id = get_tenant_shard_id(args.tenant_shard_id, env)?;

            let timeline_name_mappings = env.timeline_name_mappings();

            let mut table = comfy_table::Table::new();

            table.load_preset(comfy_table::presets::NOTHING);

            table.set_header([
                "ENDPOINT",
                "ADDRESS",
                "TIMELINE",
                "BRANCH NAME",
                "LSN",
                "STATUS",
            ]);

            for (endpoint_id, endpoint) in cplane
                .endpoints
                .iter()
                .filter(|(_, endpoint)| endpoint.tenant_id == tenant_shard_id.tenant_id)
            {
                let lsn_str = match endpoint.mode {
                    ComputeMode::Static(lsn) => {
                        // -> read-only endpoint
                        // Use the node's LSN.
                        lsn.to_string()
                    }
                    _ => {
                        // As the LSN here refers to the one that the compute is started with,
                        // we display nothing as it is a primary/hot standby compute.
                        "---".to_string()
                    }
                };

                let branch_name = timeline_name_mappings
                    .get(&TenantTimelineId::new(
                        tenant_shard_id.tenant_id,
                        endpoint.timeline_id,
                    ))
                    .map(|name| name.as_str())
                    .unwrap_or("?");

                table.add_row([
                    endpoint_id.as_str(),
                    &endpoint.pg_address.to_string(),
                    &endpoint.timeline_id.to_string(),
                    branch_name,
                    lsn_str.as_str(),
                    &format!("{}", endpoint.status()),
                ]);
            }

            println!("{table}");
        }
        EndpointCmd::Create(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let branch_name = args
                .branch_name
                .clone()
                .unwrap_or(DEFAULT_BRANCH_NAME.to_owned());
            let endpoint_id = args
                .endpoint_id
                .clone()
                .unwrap_or_else(|| format!("ep-{branch_name}"));

            let timeline_id = env
                .get_branch_timeline_id(&branch_name, tenant_id)
                .ok_or_else(|| anyhow!("Found no timeline id for branch name '{branch_name}'"))?;

            let mode = match (args.lsn, args.hot_standby) {
                (Some(lsn), false) => ComputeMode::Static(lsn),
                (None, true) => ComputeMode::Replica,
                (None, false) => ComputeMode::Primary,
                (Some(_), true) => anyhow::bail!("cannot specify both lsn and hot-standby"),
            };

            match (mode, args.hot_standby) {
                (ComputeMode::Static(_), true) => {
                    bail!(
                        "Cannot start a node in hot standby mode when it is already configured as a static replica"
                    )
                }
                (ComputeMode::Primary, true) => {
                    bail!(
                        "Cannot start a node as a hot standby replica, it is already configured as primary node"
                    )
                }
                _ => {}
            }

            if !args.allow_multiple {
                cplane.check_conflicting_endpoints(mode, tenant_id, timeline_id)?;
            }

            cplane.new_endpoint(
                &endpoint_id,
                tenant_id,
                timeline_id,
                args.pg_port,
                args.external_http_port,
                args.internal_http_port,
                args.pg_version,
                mode,
                args.grpc,
                !args.update_catalog,
                false,
                args.privileged_role_name.clone(),
            )?;
        }
        EndpointCmd::Start(args) => {
            let endpoint_id = &args.endpoint_id;
            let pageserver_id = args.endpoint_pageserver_id;
            let remote_ext_base_url = &args.remote_ext_base_url;

            let default_generation = env
                .storage_controller
                .timelines_onto_safekeepers
                .then_some(1);
            let safekeepers_generation = args
                .safekeepers_generation
                .or(default_generation)
                .map(SafekeeperGeneration::new);
            // If --safekeepers argument is given, use only the listed
            // safekeeper nodes; otherwise all from the env.
            let safekeepers = if let Some(safekeepers) = parse_safekeepers(&args.safekeepers)? {
                safekeepers
            } else {
                env.safekeepers.iter().map(|sk| sk.id).collect()
            };

            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .ok_or_else(|| anyhow!("endpoint {endpoint_id} not found"))?;

            if !args.allow_multiple {
                cplane.check_conflicting_endpoints(
                    endpoint.mode,
                    endpoint.tenant_id,
                    endpoint.timeline_id,
                )?;
            }

            let prefer_protocol = if endpoint.grpc {
                PageserverProtocol::Grpc
            } else {
                PageserverProtocol::Libpq
            };

            let mut pageserver_conninfo = if let Some(ps_id) = pageserver_id {
                let conf = env.get_pageserver_conf(ps_id).unwrap();
                local_pageserver_conf_to_conn_info(conf)?
            } else {
                // Look up the currently attached location of the tenant, and its striping metadata,
                // to pass these on to postgres.
                let storage_controller = StorageController::from_env(env);
                let locate_result = storage_controller.tenant_locate(endpoint.tenant_id).await?;
                assert!(!locate_result.shards.is_empty());

                // Initialize LSN leases for static computes.
                if let ComputeMode::Static(lsn) = endpoint.mode {
                    futures::future::try_join_all(locate_result.shards.iter().map(
                        |shard| async move {
                            let conf = env.get_pageserver_conf(shard.node_id).unwrap();
                            let pageserver = PageServerNode::from_env(env, conf);

                            pageserver
                                .http_client
                                .timeline_init_lsn_lease(shard.shard_id, endpoint.timeline_id, lsn)
                                .await
                        },
                    ))
                    .await?;
                }

                tenant_locate_response_to_conn_info(&locate_result)?
            };
            pageserver_conninfo.prefer_protocol = prefer_protocol;

            let ps_conf = env.get_pageserver_conf(DEFAULT_PAGESERVER_ID)?;
            let auth_token = if matches!(ps_conf.pg_auth_type, AuthType::NeonJWT) {
                let claims = Claims::new(Some(endpoint.tenant_id), Scope::Tenant);

                Some(env.generate_auth_token(&claims)?)
            } else {
                None
            };

            let exp = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?
                + Duration::from_secs(86400))
            .as_secs();
            let claims = endpoint_storage::claims::EndpointStorageClaims {
                tenant_id: endpoint.tenant_id,
                timeline_id: endpoint.timeline_id,
                endpoint_id: endpoint_id.to_string(),
                exp,
            };

            let endpoint_storage_token = env.generate_auth_token(&claims)?;
            let endpoint_storage_addr = env.endpoint_storage.listen_addr.to_string();

            let args = control_plane::endpoint::EndpointStartArgs {
                auth_token,
                endpoint_storage_token,
                endpoint_storage_addr,
                safekeepers_generation,
                safekeepers,
                pageserver_conninfo,
                remote_ext_base_url: remote_ext_base_url.clone(),
                create_test_user: args.create_test_user,
                start_timeout: args.start_timeout,
                autoprewarm: args.autoprewarm,
                offload_lfc_interval_seconds: args.offload_lfc_interval_seconds,
                dev: args.dev,
            };

            println!("Starting existing endpoint {endpoint_id}...");
            endpoint.start(args).await?;
        }
        EndpointCmd::UpdatePageservers(args) => {
            let endpoint_id = &args.endpoint_id;
            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;
            let prefer_protocol = if endpoint.grpc {
                PageserverProtocol::Grpc
            } else {
                PageserverProtocol::Libpq
            };
            let mut pageserver_conninfo = match args.pageserver_id {
                Some(pageserver_id) => {
                    let conf = env.get_pageserver_conf(pageserver_id)?;
                    local_pageserver_conf_to_conn_info(conf)?
                }
                None => {
                    let storage_controller = StorageController::from_env(env);
                    let locate_result =
                        storage_controller.tenant_locate(endpoint.tenant_id).await?;

                    tenant_locate_response_to_conn_info(&locate_result)?
                }
            };
            pageserver_conninfo.prefer_protocol = prefer_protocol;

            endpoint
                .update_pageservers_in_config(&pageserver_conninfo)
                .await?;
        }
        EndpointCmd::Reconfigure(args) => {
            let endpoint_id = &args.endpoint_id;
            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;

            let prefer_protocol = if endpoint.grpc {
                PageserverProtocol::Grpc
            } else {
                PageserverProtocol::Libpq
            };
            let mut pageserver_conninfo = if let Some(ps_id) = args.endpoint_pageserver_id {
                let conf = env.get_pageserver_conf(ps_id)?;
                local_pageserver_conf_to_conn_info(conf)?
            } else {
                // Look up the currently attached location of the tenant, and its striping metadata,
                // to pass these on to postgres.
                let storage_controller = StorageController::from_env(env);
                let locate_result = storage_controller.tenant_locate(endpoint.tenant_id).await?;

                tenant_locate_response_to_conn_info(&locate_result)?
            };
            pageserver_conninfo.prefer_protocol = prefer_protocol;

            // If --safekeepers argument is given, use only the listed
            // safekeeper nodes; otherwise all from the env.
            let safekeepers = parse_safekeepers(&args.safekeepers)?;
            endpoint
                .reconfigure(Some(&pageserver_conninfo), safekeepers, None)
                .await?;
        }
        EndpointCmd::RefreshConfiguration(args) => {
            let endpoint_id = &args.endpoint_id;
            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;
            endpoint.refresh_configuration().await?;
        }
        EndpointCmd::Stop(args) => {
            let endpoint_id = &args.endpoint_id;
            let endpoint = cplane
                .endpoints
                .get(endpoint_id)
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;
            match endpoint.stop(args.mode, args.destroy).await?.lsn {
                Some(lsn) => println!("{lsn}"),
                None => println!("null"),
            }
        }
        EndpointCmd::GenerateJwt(args) => {
            let endpoint = {
                let endpoint_id = &args.endpoint_id;

                cplane
                    .endpoints
                    .get(endpoint_id)
                    .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?
            };

            let jwt = endpoint.generate_jwt(args.scope)?;

            print!("{jwt}");
        }
    }

    Ok(())
}

/// Parse --safekeepers as list of safekeeper ids.
fn parse_safekeepers(safekeepers_str: &Option<String>) -> Result<Option<Vec<NodeId>>> {
    if let Some(safekeepers_str) = safekeepers_str {
        let mut safekeepers: Vec<NodeId> = Vec::new();
        for sk_id in safekeepers_str.split(',').map(str::trim) {
            let sk_id = NodeId(
                u64::from_str(sk_id)
                    .map_err(|_| anyhow!("invalid node ID \"{sk_id}\" in --safekeepers list"))?,
            );
            safekeepers.push(sk_id);
        }
        Ok(Some(safekeepers))
    } else {
        Ok(None)
    }
}

fn handle_mappings(subcmd: &MappingsCmd, env: &mut local_env::LocalEnv) -> Result<()> {
    match subcmd {
        MappingsCmd::Map(args) => {
            env.register_branch_mapping(
                args.branch_name.to_owned(),
                args.tenant_id,
                args.timeline_id,
            )?;

            Ok(())
        }
    }
}

fn get_pageserver(
    env: &local_env::LocalEnv,
    pageserver_id_arg: Option<NodeId>,
) -> Result<PageServerNode> {
    let node_id = pageserver_id_arg.unwrap_or(DEFAULT_PAGESERVER_ID);

    Ok(PageServerNode::from_env(
        env,
        env.get_pageserver_conf(node_id)?,
    ))
}

async fn handle_pageserver(subcmd: &PageserverCmd, env: &local_env::LocalEnv) -> Result<()> {
    match subcmd {
        PageserverCmd::Start(args) => {
            if let Err(e) = get_pageserver(env, args.pageserver_id)?
                .start(&args.start_timeout)
                .await
            {
                eprintln!("pageserver start failed: {e}");
                exit(1);
            }
        }

        PageserverCmd::Stop(args) => {
            let immediate = match args.stop_mode {
                StopMode::Fast => false,
                StopMode::Immediate => true,
            };
            if let Err(e) = get_pageserver(env, args.pageserver_id)?.stop(immediate) {
                eprintln!("pageserver stop failed: {e}");
                exit(1);
            }
        }

        PageserverCmd::Restart(args) => {
            let pageserver = get_pageserver(env, args.pageserver_id)?;
            //TODO what shutdown strategy should we use here?
            if let Err(e) = pageserver.stop(false) {
                eprintln!("pageserver stop failed: {e}");
                exit(1);
            }

            if let Err(e) = pageserver.start(&args.start_timeout).await {
                eprintln!("pageserver start failed: {e}");
                exit(1);
            }
        }

        PageserverCmd::Status(args) => {
            match get_pageserver(env, args.pageserver_id)?
                .check_status()
                .await
            {
                Ok(_) => println!("Page server is up and running"),
                Err(err) => {
                    eprintln!("Page server is not available: {err}");
                    exit(1);
                }
            }
        }
    }
    Ok(())
}

async fn handle_storage_controller(
    subcmd: &StorageControllerCmd,
    env: &local_env::LocalEnv,
) -> Result<()> {
    let svc = StorageController::from_env(env);
    match subcmd {
        StorageControllerCmd::Start(args) => {
            let start_args = NeonStorageControllerStartArgs {
                instance_id: args.instance_id,
                base_port: args.base_port,
                start_timeout: args.start_timeout,
                handle_ps_local_disk_loss: args.handle_ps_local_disk_loss,
            };

            if let Err(e) = svc.start(start_args).await {
                eprintln!("start failed: {e}");
                exit(1);
            }
        }

        StorageControllerCmd::Stop(args) => {
            let stop_args = NeonStorageControllerStopArgs {
                instance_id: args.instance_id,
                immediate: match args.stop_mode {
                    StopMode::Fast => false,
                    StopMode::Immediate => true,
                },
            };
            if let Err(e) = svc.stop(stop_args).await {
                eprintln!("stop failed: {e}");
                exit(1);
            }
        }
    }
    Ok(())
}

fn get_safekeeper(env: &local_env::LocalEnv, id: NodeId) -> Result<SafekeeperNode> {
    if let Some(node) = env.safekeepers.iter().find(|node| node.id == id) {
        Ok(SafekeeperNode::from_env(env, node))
    } else {
        bail!("could not find safekeeper {id}")
    }
}

async fn handle_safekeeper(subcmd: &SafekeeperCmd, env: &local_env::LocalEnv) -> Result<()> {
    match subcmd {
        SafekeeperCmd::Start(args) => {
            let safekeeper = get_safekeeper(env, args.id)?;

            if let Err(e) = safekeeper.start(&args.extra_opt, &args.start_timeout).await {
                eprintln!("safekeeper start failed: {e}");
                exit(1);
            }
        }

        SafekeeperCmd::Stop(args) => {
            let safekeeper = get_safekeeper(env, args.id)?;
            let immediate = match args.stop_mode {
                StopMode::Fast => false,
                StopMode::Immediate => true,
            };
            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {e}");
                exit(1);
            }
        }

        SafekeeperCmd::Restart(args) => {
            let safekeeper = get_safekeeper(env, args.id)?;
            let immediate = match args.stop_mode {
                StopMode::Fast => false,
                StopMode::Immediate => true,
            };

            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {e}");
                exit(1);
            }

            if let Err(e) = safekeeper.start(&args.extra_opt, &args.start_timeout).await {
                eprintln!("safekeeper start failed: {e}");
                exit(1);
            }
        }
    }
    Ok(())
}

async fn handle_endpoint_storage(
    subcmd: &EndpointStorageCmd,
    env: &local_env::LocalEnv,
) -> Result<()> {
    use EndpointStorageCmd::*;
    let storage = EndpointStorage::from_env(env);

    // In tests like test_forward_compatibility or test_graceful_cluster_restart
    // old neon binaries (without endpoint_storage) are present
    if !storage.bin.exists() {
        eprintln!(
            "{} binary not found. Ignore if this is a compatibility test",
            storage.bin
        );
        return Ok(());
    }

    match subcmd {
        Start(EndpointStorageStartCmd { start_timeout }) => {
            if let Err(e) = storage.start(start_timeout).await {
                eprintln!("endpoint_storage start failed: {e}");
                exit(1);
            }
        }
        Stop(EndpointStorageStopCmd { stop_mode }) => {
            let immediate = match stop_mode {
                StopMode::Fast => false,
                StopMode::Immediate => true,
            };
            if let Err(e) = storage.stop(immediate) {
                eprintln!("proxy stop failed: {e}");
                exit(1);
            }
        }
    };
    Ok(())
}

async fn handle_storage_broker(subcmd: &StorageBrokerCmd, env: &local_env::LocalEnv) -> Result<()> {
    match subcmd {
        StorageBrokerCmd::Start(args) => {
            let storage_broker = StorageBroker::from_env(env);
            if let Err(e) = storage_broker.start(&args.start_timeout).await {
                eprintln!("broker start failed: {e}");
                exit(1);
            }
        }

        StorageBrokerCmd::Stop(_args) => {
            // FIXME: stop_mode unused
            let storage_broker = StorageBroker::from_env(env);
            if let Err(e) = storage_broker.stop() {
                eprintln!("broker stop failed: {e}");
                exit(1);
            }
        }
    }
    Ok(())
}

async fn handle_start_all(
    args: &StartCmdArgs,
    env: &'static local_env::LocalEnv,
) -> anyhow::Result<()> {
    // FIXME: this was called "retry_timeout", is it right?
    let Err(errors) = handle_start_all_impl(env, args.timeout).await else {
        neon_start_status_check(env, args.timeout.as_ref())
            .await
            .context("status check after successful startup of all services")?;
        return Ok(());
    };

    eprintln!("startup failed because one or more services could not be started");

    for e in errors {
        eprintln!("{e}");
        let debug_repr = format!("{e:?}");
        for line in debug_repr.lines() {
            eprintln!("  {line}");
        }
    }

    try_stop_all(env, true).await;

    exit(2);
}

/// Returns Ok() if and only if all services could be started successfully.
/// Otherwise, returns the list of errors that occurred during startup.
async fn handle_start_all_impl(
    env: &'static local_env::LocalEnv,
    retry_timeout: humantime::Duration,
) -> Result<(), Vec<anyhow::Error>> {
    // Endpoints are not started automatically

    let mut js = JoinSet::new();

    // force infalliblity through closure
    #[allow(clippy::redundant_closure_call)]
    (|| {
        js.spawn(async move {
            let storage_broker = StorageBroker::from_env(env);
            storage_broker
                .start(&retry_timeout)
                .await
                .map_err(|e| e.context("start storage_broker"))
        });

        js.spawn(async move {
            let storage_controller = StorageController::from_env(env);
            storage_controller
                .start(NeonStorageControllerStartArgs::with_default_instance_id(
                    retry_timeout,
                ))
                .await
                .map_err(|e| e.context("start storage_controller"))
        });

        for ps_conf in &env.pageservers {
            js.spawn(async move {
                let pageserver = PageServerNode::from_env(env, ps_conf);
                pageserver
                    .start(&retry_timeout)
                    .await
                    .map_err(|e| e.context(format!("start pageserver {}", ps_conf.id)))
            });
        }

        for node in env.safekeepers.iter() {
            js.spawn(async move {
                let safekeeper = SafekeeperNode::from_env(env, node);
                safekeeper
                    .start(&[], &retry_timeout)
                    .await
                    .map_err(|e| e.context(format!("start safekeeper {}", safekeeper.id)))
            });
        }

        js.spawn(async move {
            EndpointStorage::from_env(env)
                .start(&retry_timeout)
                .await
                .map_err(|e| e.context("start endpoint_storage"))
        });
    })();

    let mut errors = Vec::new();
    while let Some(result) = js.join_next().await {
        let result = result.expect("we don't panic or cancel the tasks");
        if let Err(e) = result {
            errors.push(e);
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    Ok(())
}

async fn neon_start_status_check(
    env: &local_env::LocalEnv,
    retry_timeout: &Duration,
) -> anyhow::Result<()> {
    const RETRY_INTERVAL: Duration = Duration::from_millis(100);
    const NOTICE_AFTER_RETRIES: Duration = Duration::from_secs(5);

    let storcon = StorageController::from_env(env);

    let retries = retry_timeout.as_millis() / RETRY_INTERVAL.as_millis();
    let notice_after_retries = retry_timeout.as_millis() / NOTICE_AFTER_RETRIES.as_millis();

    println!("\nRunning neon status check");

    for retry in 0..retries {
        if retry == notice_after_retries {
            println!("\nNeon status check has not passed yet, continuing to wait")
        }

        let mut passed = true;
        let mut nodes = storcon.node_list().await?;
        let mut pageservers = env.pageservers.clone();

        if nodes.len() != pageservers.len() {
            continue;
        }

        nodes.sort_by_key(|ps| ps.id);
        pageservers.sort_by_key(|ps| ps.id);

        for (idx, pageserver) in pageservers.iter().enumerate() {
            let node = &nodes[idx];
            if node.id != pageserver.id {
                passed = false;
                break;
            }

            if !matches!(node.availability, NodeAvailabilityWrapper::Active) {
                passed = false;
                break;
            }
        }

        if passed {
            println!("\nNeon started and passed status check");
            return Ok(());
        }

        tokio::time::sleep(RETRY_INTERVAL).await;
    }

    anyhow::bail!("\nNeon passed status check")
}

async fn handle_stop_all(args: &StopCmdArgs, env: &local_env::LocalEnv) -> Result<()> {
    let immediate = match args.mode {
        StopMode::Fast => false,
        StopMode::Immediate => true,
    };

    try_stop_all(env, immediate).await;

    Ok(())
}

async fn try_stop_all(env: &local_env::LocalEnv, immediate: bool) {
    let mode = if immediate {
        EndpointTerminateMode::Immediate
    } else {
        EndpointTerminateMode::Fast
    };
    // Stop all endpoints
    match ComputeControlPlane::load(env.clone()) {
        Ok(cplane) => {
            for (_k, node) in cplane.endpoints {
                if let Err(e) = node.stop(mode, false).await {
                    eprintln!("postgres stop failed: {e:#}");
                }
            }
        }
        Err(e) => {
            eprintln!("postgres stop failed, could not restore control plane data from env: {e:#}")
        }
    }

    let storage = EndpointStorage::from_env(env);
    if let Err(e) = storage.stop(immediate) {
        eprintln!("endpoint_storage stop failed: {e:#}");
    }

    for ps_conf in &env.pageservers {
        let pageserver = PageServerNode::from_env(env, ps_conf);
        if let Err(e) = pageserver.stop(immediate) {
            eprintln!("pageserver {} stop failed: {:#}", ps_conf.id, e);
        }
    }

    for node in env.safekeepers.iter() {
        let safekeeper = SafekeeperNode::from_env(env, node);
        if let Err(e) = safekeeper.stop(immediate) {
            eprintln!("safekeeper {} stop failed: {:#}", safekeeper.id, e);
        }
    }

    let storage_broker = StorageBroker::from_env(env);
    if let Err(e) = storage_broker.stop() {
        eprintln!("neon broker stop failed: {e:#}");
    }

    // Stop all storage controller instances. In the most common case there's only one,
    // but iterate though the base data directory in order to discover the instances.
    let storcon_instances = env
        .storage_controller_instances()
        .await
        .expect("Must inspect data dir");
    for (instance_id, _instance_dir_path) in storcon_instances {
        let storage_controller = StorageController::from_env(env);
        let stop_args = NeonStorageControllerStopArgs {
            instance_id,
            immediate,
        };

        if let Err(e) = storage_controller.stop(stop_args).await {
            eprintln!("Storage controller instance {instance_id} stop failed: {e:#}");
        }
    }
}
