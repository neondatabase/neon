//!
//! `neon_local` is an executable that can be used to create a local
//! Neon environment, for testing purposes. The local environment is
//! quite different from the cloud environment with Kubernetes, but it
//! easier to work with locally. The python tests in `test_runner`
//! rely on `neon_local` to set up the environment for each test.
//!
use anyhow::{anyhow, bail, Context, Result};
use clap::{value_parser, Arg, ArgAction, ArgMatches, Command, ValueEnum};
use compute_api::spec::ComputeMode;
use control_plane::endpoint::ComputeControlPlane;
use control_plane::local_env::{
    InitForceMode, LocalEnv, NeonBroker, NeonLocalInitConf, NeonLocalInitPageserverConf,
    SafekeeperConf,
};
use control_plane::pageserver::PageServerNode;
use control_plane::safekeeper::SafekeeperNode;
use control_plane::storage_controller::StorageController;
use control_plane::{broker, local_env};
use pageserver_api::config::defaults::{
    DEFAULT_HTTP_LISTEN_PORT as DEFAULT_PAGESERVER_HTTP_PORT,
    DEFAULT_PG_LISTEN_PORT as DEFAULT_PAGESERVER_PG_PORT,
};
use pageserver_api::controller_api::PlacementPolicy;
use pageserver_api::models::{
    ShardParameters, TenantCreateRequest, TimelineCreateRequest, TimelineInfo,
};
use pageserver_api::shard::{ShardCount, ShardStripeSize, TenantShardId};
use postgres_backend::AuthType;
use postgres_connection::parse_host_port;
use safekeeper_api::{
    DEFAULT_HTTP_LISTEN_PORT as DEFAULT_SAFEKEEPER_HTTP_PORT,
    DEFAULT_PG_LISTEN_PORT as DEFAULT_SAFEKEEPER_PG_PORT,
};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use std::time::Duration;
use storage_broker::DEFAULT_LISTEN_ADDR as DEFAULT_BROKER_ADDR;
use url::Host;
use utils::{
    auth::{Claims, Scope},
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
    project_git_version,
};

// Default id of a safekeeper node, if not specified on the command line.
const DEFAULT_SAFEKEEPER_ID: NodeId = NodeId(1);
const DEFAULT_PAGESERVER_ID: NodeId = NodeId(1);
const DEFAULT_BRANCH_NAME: &str = "main";
project_git_version!(GIT_VERSION);

const DEFAULT_PG_VERSION: &str = "15";

const DEFAULT_PAGESERVER_CONTROL_PLANE_API: &str = "http://127.0.0.1:1234/upcall/v1/";

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

// Main entry point for the 'neon_local' CLI utility
//
// This utility helps to manage neon installation. That includes following:
//   * Management of local postgres installations running on top of the
//     pageserver.
//   * Providing CLI api to the pageserver
//   * TODO: export/import to/from usual postgres
fn main() -> Result<()> {
    let matches = cli().get_matches();

    let (sub_name, sub_args) = match matches.subcommand() {
        Some(subcommand_data) => subcommand_data,
        None => bail!("no subcommand provided"),
    };

    // Check for 'neon init' command first.
    let subcommand_result = if sub_name == "init" {
        handle_init(sub_args).map(Some)
    } else {
        // all other commands need an existing config
        let mut env =
            LocalEnv::load_config(&local_env::base_path()).context("Error loading config")?;
        let original_env = env.clone();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let subcommand_result = match sub_name {
            "tenant" => rt.block_on(handle_tenant(sub_args, &mut env)),
            "timeline" => rt.block_on(handle_timeline(sub_args, &mut env)),
            "start" => rt.block_on(handle_start_all(&env, get_start_timeout(sub_args))),
            "stop" => rt.block_on(handle_stop_all(sub_args, &env)),
            "pageserver" => rt.block_on(handle_pageserver(sub_args, &env)),
            "storage_controller" => rt.block_on(handle_storage_controller(sub_args, &env)),
            "safekeeper" => rt.block_on(handle_safekeeper(sub_args, &env)),
            "endpoint" => rt.block_on(handle_endpoint(sub_args, &env)),
            "mappings" => handle_mappings(sub_args, &mut env),
            "pg" => bail!("'pg' subcommand has been renamed to 'endpoint'"),
            _ => bail!("unexpected subcommand {sub_name}"),
        };

        if original_env != env {
            subcommand_result.map(|()| Some(env))
        } else {
            subcommand_result.map(|()| None)
        }
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

        print!("{} @{}: ", br_sym, ancestor_lsn);
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

/// Returns a map of timeline IDs to timeline_id@lsn strings.
/// Connects to the pageserver to query this information.
async fn get_timeline_infos(
    env: &local_env::LocalEnv,
    tenant_shard_id: &TenantShardId,
) -> Result<HashMap<TimelineId, TimelineInfo>> {
    Ok(get_default_pageserver(env)
        .timeline_list(tenant_shard_id)
        .await?
        .into_iter()
        .map(|timeline_info| (timeline_info.timeline_id, timeline_info))
        .collect())
}

// Helper function to parse --tenant_id option, or get the default from config file
fn get_tenant_id(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> anyhow::Result<TenantId> {
    if let Some(tenant_id_from_arguments) = parse_tenant_id(sub_match).transpose() {
        tenant_id_from_arguments
    } else if let Some(default_id) = env.default_tenant_id {
        Ok(default_id)
    } else {
        anyhow::bail!("No tenant id. Use --tenant-id, or set a default tenant");
    }
}

// Helper function to parse --tenant_id option, for commands that accept a shard suffix
fn get_tenant_shard_id(
    sub_match: &ArgMatches,
    env: &local_env::LocalEnv,
) -> anyhow::Result<TenantShardId> {
    if let Some(tenant_id_from_arguments) = parse_tenant_shard_id(sub_match).transpose() {
        tenant_id_from_arguments
    } else if let Some(default_id) = env.default_tenant_id {
        Ok(TenantShardId::unsharded(default_id))
    } else {
        anyhow::bail!("No tenant shard id. Use --tenant-id, or set a default tenant");
    }
}

fn parse_tenant_id(sub_match: &ArgMatches) -> anyhow::Result<Option<TenantId>> {
    sub_match
        .get_one::<String>("tenant-id")
        .map(|tenant_id| TenantId::from_str(tenant_id))
        .transpose()
        .context("Failed to parse tenant id from the argument string")
}

fn parse_tenant_shard_id(sub_match: &ArgMatches) -> anyhow::Result<Option<TenantShardId>> {
    sub_match
        .get_one::<String>("tenant-id")
        .map(|id_str| TenantShardId::from_str(id_str))
        .transpose()
        .context("Failed to parse tenant shard id from the argument string")
}

fn parse_timeline_id(sub_match: &ArgMatches) -> anyhow::Result<Option<TimelineId>> {
    sub_match
        .get_one::<String>("timeline-id")
        .map(|timeline_id| TimelineId::from_str(timeline_id))
        .transpose()
        .context("Failed to parse timeline id from the argument string")
}

fn handle_init(init_match: &ArgMatches) -> anyhow::Result<LocalEnv> {
    let num_pageservers = init_match.get_one::<u16>("num-pageservers");

    let force = init_match.get_one("force").expect("we set a default value");

    // Create the in-memory `LocalEnv` that we'd normally load from disk in `load_config`.
    let init_conf: NeonLocalInitConf = if let Some(config_path) =
        init_match.get_one::<PathBuf>("config")
    {
        // User (likely the Python test suite) provided a description of the environment.
        if num_pageservers.is_some() {
            bail!("Cannot specify both --num-pageservers and --config, use key `pageservers` in the --config file instead");
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
            control_plane_api: Some(Some(DEFAULT_PAGESERVER_CONTROL_PLANE_API.parse().unwrap())),
            broker: NeonBroker {
                listen_addr: DEFAULT_BROKER_ADDR.parse().unwrap(),
            },
            safekeepers: vec![SafekeeperConf {
                id: DEFAULT_SAFEKEEPER_ID,
                pg_port: DEFAULT_SAFEKEEPER_PG_PORT,
                http_port: DEFAULT_SAFEKEEPER_HTTP_PORT,
                ..Default::default()
            }],
            pageservers: (0..num_pageservers.copied().unwrap_or(1))
                .map(|i| {
                    let pageserver_id = NodeId(DEFAULT_PAGESERVER_ID.0 + i as u64);
                    let pg_port = DEFAULT_PAGESERVER_PG_PORT + i;
                    let http_port = DEFAULT_PAGESERVER_HTTP_PORT + i;
                    NeonLocalInitPageserverConf {
                        id: pageserver_id,
                        listen_pg_addr: format!("127.0.0.1:{pg_port}"),
                        listen_http_addr: format!("127.0.0.1:{http_port}"),
                        pg_auth_type: AuthType::Trust,
                        http_auth_type: AuthType::Trust,
                        other: Default::default(),
                    }
                })
                .collect(),
            pg_distrib_dir: None,
            neon_distrib_dir: None,
            default_tenant_id: TenantId::from_array(std::array::from_fn(|_| 0)),
            storage_controller: None,
            control_plane_compute_hook_api: None,
        }
    };

    LocalEnv::init(init_conf, force)
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

async fn handle_tenant(
    tenant_match: &ArgMatches,
    env: &mut local_env::LocalEnv,
) -> anyhow::Result<()> {
    let pageserver = get_default_pageserver(env);
    match tenant_match.subcommand() {
        Some(("list", _)) => {
            for t in pageserver.tenant_list().await? {
                println!("{} {:?}", t.id, t.state);
            }
        }
        Some(("import", import_match)) => {
            let tenant_id = parse_tenant_id(import_match)?.unwrap_or_else(TenantId::generate);

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
        Some(("create", create_match)) => {
            let tenant_conf: HashMap<_, _> = create_match
                .get_many::<String>("config")
                .map(|vals: clap::parser::ValuesRef<'_, String>| {
                    vals.flat_map(|c| c.split_once(':')).collect()
                })
                .unwrap_or_default();

            let shard_count: u8 = create_match
                .get_one::<u8>("shard-count")
                .cloned()
                .unwrap_or(0);

            let shard_stripe_size: Option<u32> =
                create_match.get_one::<u32>("shard-stripe-size").cloned();

            let placement_policy = match create_match.get_one::<String>("placement-policy") {
                Some(s) if !s.is_empty() => serde_json::from_str::<PlacementPolicy>(s)?,
                _ => PlacementPolicy::Attached(0),
            };

            let tenant_conf = PageServerNode::parse_config(tenant_conf)?;

            // If tenant ID was not specified, generate one
            let tenant_id = parse_tenant_id(create_match)?.unwrap_or_else(TenantId::generate);

            // We must register the tenant with the storage controller, so
            // that when the pageserver restarts, it will be re-attached.
            let storage_controller = StorageController::from_env(env);
            storage_controller
                .tenant_create(TenantCreateRequest {
                    // Note that ::unsharded here isn't actually because the tenant is unsharded, its because the
                    // storage controller expecfs a shard-naive tenant_id in this attribute, and the TenantCreateRequest
                    // type is used both in storage controller (for creating tenants) and in pageserver (for creating shards)
                    new_tenant_id: TenantShardId::unsharded(tenant_id),
                    generation: None,
                    shard_parameters: ShardParameters {
                        count: ShardCount::new(shard_count),
                        stripe_size: shard_stripe_size
                            .map(ShardStripeSize)
                            .unwrap_or(ShardParameters::DEFAULT_STRIPE_SIZE),
                    },
                    placement_policy: Some(placement_policy),
                    config: tenant_conf,
                })
                .await?;
            println!("tenant {tenant_id} successfully created on the pageserver");

            // Create an initial timeline for the new tenant
            let new_timeline_id =
                parse_timeline_id(create_match)?.unwrap_or(TimelineId::generate());
            let pg_version = create_match
                .get_one::<u32>("pg-version")
                .copied()
                .context("Failed to parse postgres version from the argument string")?;

            // FIXME: passing None for ancestor_start_lsn is not kosher in a sharded world: we can't have
            // different shards picking different start lsns.  Maybe we have to teach storage controller
            // to let shard 0 branch first and then propagate the chosen LSN to other shards.
            storage_controller
                .tenant_timeline_create(
                    tenant_id,
                    TimelineCreateRequest {
                        new_timeline_id,
                        ancestor_timeline_id: None,
                        ancestor_start_lsn: None,
                        existing_initdb_timeline_id: None,
                        pg_version: Some(pg_version),
                    },
                )
                .await?;

            env.register_branch_mapping(
                DEFAULT_BRANCH_NAME.to_string(),
                tenant_id,
                new_timeline_id,
            )?;

            println!("Created an initial timeline '{new_timeline_id}' for tenant: {tenant_id}",);

            if create_match.get_flag("set-default") {
                println!("Setting tenant {tenant_id} as a default one");
                env.default_tenant_id = Some(tenant_id);
            }
        }
        Some(("set-default", set_default_match)) => {
            let tenant_id =
                parse_tenant_id(set_default_match)?.context("No tenant id specified")?;
            println!("Setting tenant {tenant_id} as a default one");
            env.default_tenant_id = Some(tenant_id);
        }
        Some(("config", create_match)) => {
            let tenant_id = get_tenant_id(create_match, env)?;
            let tenant_conf: HashMap<_, _> = create_match
                .get_many::<String>("config")
                .map(|vals| vals.flat_map(|c| c.split_once(':')).collect())
                .unwrap_or_default();

            pageserver
                .tenant_config(tenant_id, tenant_conf)
                .await
                .with_context(|| format!("Tenant config failed for tenant with id {tenant_id}"))?;
            println!("tenant {tenant_id} successfully configured on the pageserver");
        }

        Some((sub_name, _)) => bail!("Unexpected tenant subcommand '{}'", sub_name),
        None => bail!("no tenant subcommand provided"),
    }
    Ok(())
}

async fn handle_timeline(timeline_match: &ArgMatches, env: &mut local_env::LocalEnv) -> Result<()> {
    let pageserver = get_default_pageserver(env);

    match timeline_match.subcommand() {
        Some(("list", list_match)) => {
            // TODO(sharding): this command shouldn't have to specify a shard ID: we should ask the storage controller
            // where shard 0 is attached, and query there.
            let tenant_shard_id = get_tenant_shard_id(list_match, env)?;
            let timelines = pageserver.timeline_list(&tenant_shard_id).await?;
            print_timelines_tree(timelines, env.timeline_name_mappings())?;
        }
        Some(("create", create_match)) => {
            let tenant_id = get_tenant_id(create_match, env)?;
            let new_branch_name = create_match
                .get_one::<String>("branch-name")
                .ok_or_else(|| anyhow!("No branch name provided"))?;

            let pg_version = create_match
                .get_one::<u32>("pg-version")
                .copied()
                .context("Failed to parse postgres version from the argument string")?;

            let new_timeline_id_opt = parse_timeline_id(create_match)?;
            let new_timeline_id = new_timeline_id_opt.unwrap_or(TimelineId::generate());

            let storage_controller = StorageController::from_env(env);
            let create_req = TimelineCreateRequest {
                new_timeline_id,
                ancestor_timeline_id: None,
                existing_initdb_timeline_id: None,
                ancestor_start_lsn: None,
                pg_version: Some(pg_version),
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
        Some(("import", import_match)) => {
            let tenant_id = get_tenant_id(import_match, env)?;
            let timeline_id = parse_timeline_id(import_match)?.expect("No timeline id provided");
            let branch_name = import_match
                .get_one::<String>("branch-name")
                .ok_or_else(|| anyhow!("No branch name provided"))?;

            // Parse base inputs
            let base_tarfile = import_match
                .get_one::<PathBuf>("base-tarfile")
                .ok_or_else(|| anyhow!("No base-tarfile provided"))?
                .to_owned();
            let base_lsn = Lsn::from_str(
                import_match
                    .get_one::<String>("base-lsn")
                    .ok_or_else(|| anyhow!("No base-lsn provided"))?,
            )?;
            let base = (base_lsn, base_tarfile);

            // Parse pg_wal inputs
            let wal_tarfile = import_match.get_one::<PathBuf>("wal-tarfile").cloned();
            let end_lsn = import_match
                .get_one::<String>("end-lsn")
                .map(|s| Lsn::from_str(s).unwrap());
            // TODO validate both or none are provided
            let pg_wal = end_lsn.zip(wal_tarfile);

            let pg_version = import_match
                .get_one::<u32>("pg-version")
                .copied()
                .context("Failed to parse postgres version from the argument string")?;

            println!("Importing timeline into pageserver ...");
            pageserver
                .timeline_import(tenant_id, timeline_id, base, pg_wal, pg_version)
                .await?;
            env.register_branch_mapping(branch_name.to_string(), tenant_id, timeline_id)?;
            println!("Done");
        }
        Some(("branch", branch_match)) => {
            let tenant_id = get_tenant_id(branch_match, env)?;
            let new_branch_name = branch_match
                .get_one::<String>("branch-name")
                .ok_or_else(|| anyhow!("No branch name provided"))?;
            let ancestor_branch_name = branch_match
                .get_one::<String>("ancestor-branch-name")
                .map(|s| s.as_str())
                .unwrap_or(DEFAULT_BRANCH_NAME);
            let ancestor_timeline_id = env
                .get_branch_timeline_id(ancestor_branch_name, tenant_id)
                .ok_or_else(|| {
                    anyhow!("Found no timeline id for branch name '{ancestor_branch_name}'")
                })?;

            let start_lsn = branch_match
                .get_one::<String>("ancestor-start-lsn")
                .map(|lsn_str| Lsn::from_str(lsn_str))
                .transpose()
                .context("Failed to parse ancestor start Lsn from the request")?;
            let new_timeline_id = TimelineId::generate();
            let storage_controller = StorageController::from_env(env);
            let create_req = TimelineCreateRequest {
                new_timeline_id,
                ancestor_timeline_id: Some(ancestor_timeline_id),
                existing_initdb_timeline_id: None,
                ancestor_start_lsn: start_lsn,
                pg_version: None,
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
        Some((sub_name, _)) => bail!("Unexpected tenant subcommand '{sub_name}'"),
        None => bail!("no tenant subcommand provided"),
    }

    Ok(())
}

async fn handle_endpoint(ep_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let (sub_name, sub_args) = match ep_match.subcommand() {
        Some(ep_subcommand_data) => ep_subcommand_data,
        None => bail!("no endpoint subcommand provided"),
    };
    let mut cplane = ComputeControlPlane::load(env.clone())?;

    match sub_name {
        "list" => {
            // TODO(sharding): this command shouldn't have to specify a shard ID: we should ask the storage controller
            // where shard 0 is attached, and query there.
            let tenant_shard_id = get_tenant_shard_id(sub_args, env)?;
            let timeline_infos = get_timeline_infos(env, &tenant_shard_id)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Failed to load timeline info: {}", e);
                    HashMap::new()
                });

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
                        // -> primary endpoint or hot replica
                        // Use the LSN at the end of the timeline.
                        timeline_infos
                            .get(&endpoint.timeline_id)
                            .map(|bi| bi.last_record_lsn.to_string())
                            .unwrap_or_else(|| "?".to_string())
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
        "create" => {
            let tenant_id = get_tenant_id(sub_args, env)?;
            let branch_name = sub_args
                .get_one::<String>("branch-name")
                .map(|s| s.as_str())
                .unwrap_or(DEFAULT_BRANCH_NAME);
            let endpoint_id = sub_args
                .get_one::<String>("endpoint_id")
                .map(String::to_string)
                .unwrap_or_else(|| format!("ep-{branch_name}"));
            let update_catalog = sub_args
                .get_one::<bool>("update-catalog")
                .cloned()
                .unwrap_or_default();

            let lsn = sub_args
                .get_one::<String>("lsn")
                .map(|lsn_str| Lsn::from_str(lsn_str))
                .transpose()
                .context("Failed to parse Lsn from the request")?;
            let timeline_id = env
                .get_branch_timeline_id(branch_name, tenant_id)
                .ok_or_else(|| anyhow!("Found no timeline id for branch name '{branch_name}'"))?;

            let pg_port: Option<u16> = sub_args.get_one::<u16>("pg-port").copied();
            let http_port: Option<u16> = sub_args.get_one::<u16>("http-port").copied();
            let pg_version = sub_args
                .get_one::<u32>("pg-version")
                .copied()
                .context("Failed to parse postgres version from the argument string")?;

            let hot_standby = sub_args
                .get_one::<bool>("hot-standby")
                .copied()
                .unwrap_or(false);

            let allow_multiple = sub_args.get_flag("allow-multiple");

            let mode = match (lsn, hot_standby) {
                (Some(lsn), false) => ComputeMode::Static(lsn),
                (None, true) => ComputeMode::Replica,
                (None, false) => ComputeMode::Primary,
                (Some(_), true) => anyhow::bail!("cannot specify both lsn and hot-standby"),
            };

            match (mode, hot_standby) {
                (ComputeMode::Static(_), true) => {
                    bail!("Cannot start a node in hot standby mode when it is already configured as a static replica")
                }
                (ComputeMode::Primary, true) => {
                    bail!("Cannot start a node as a hot standby replica, it is already configured as primary node")
                }
                _ => {}
            }

            if !allow_multiple {
                cplane.check_conflicting_endpoints(mode, tenant_id, timeline_id)?;
            }

            cplane.new_endpoint(
                &endpoint_id,
                tenant_id,
                timeline_id,
                pg_port,
                http_port,
                pg_version,
                mode,
                !update_catalog,
            )?;
        }
        "start" => {
            let endpoint_id = sub_args
                .get_one::<String>("endpoint_id")
                .ok_or_else(|| anyhow!("No endpoint ID was provided to start"))?;

            let pageserver_id =
                if let Some(id_str) = sub_args.get_one::<String>("endpoint-pageserver-id") {
                    Some(NodeId(
                        id_str.parse().context("while parsing pageserver id")?,
                    ))
                } else {
                    None
                };

            let remote_ext_config = sub_args.get_one::<String>("remote-ext-config");

            let allow_multiple = sub_args.get_flag("allow-multiple");

            // If --safekeepers argument is given, use only the listed safekeeper nodes.
            let safekeepers =
                if let Some(safekeepers_str) = sub_args.get_one::<String>("safekeepers") {
                    let mut safekeepers: Vec<NodeId> = Vec::new();
                    for sk_id in safekeepers_str.split(',').map(str::trim) {
                        let sk_id = NodeId(u64::from_str(sk_id).map_err(|_| {
                            anyhow!("invalid node ID \"{sk_id}\" in --safekeepers list")
                        })?);
                        safekeepers.push(sk_id);
                    }
                    safekeepers
                } else {
                    env.safekeepers.iter().map(|sk| sk.id).collect()
                };

            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .ok_or_else(|| anyhow::anyhow!("endpoint {endpoint_id} not found"))?;

            let create_test_user = sub_args
                .get_one::<bool>("create-test-user")
                .cloned()
                .unwrap_or_default();

            if !allow_multiple {
                cplane.check_conflicting_endpoints(
                    endpoint.mode,
                    endpoint.tenant_id,
                    endpoint.timeline_id,
                )?;
            }

            let (pageservers, stripe_size) = if let Some(pageserver_id) = pageserver_id {
                let conf = env.get_pageserver_conf(pageserver_id).unwrap();
                let parsed = parse_host_port(&conf.listen_pg_addr).expect("Bad config");
                (
                    vec![(parsed.0, parsed.1.unwrap_or(5432))],
                    // If caller is telling us what pageserver to use, this is not a tenant which is
                    // full managed by storage controller, therefore not sharded.
                    ShardParameters::DEFAULT_STRIPE_SIZE,
                )
            } else {
                // Look up the currently attached location of the tenant, and its striping metadata,
                // to pass these on to postgres.
                let storage_controller = StorageController::from_env(env);
                let locate_result = storage_controller.tenant_locate(endpoint.tenant_id).await?;
                let pageservers = locate_result
                    .shards
                    .into_iter()
                    .map(|shard| {
                        (
                            Host::parse(&shard.listen_pg_addr)
                                .expect("Storage controller reported bad hostname"),
                            shard.listen_pg_port,
                        )
                    })
                    .collect::<Vec<_>>();
                let stripe_size = locate_result.shard_params.stripe_size;

                (pageservers, stripe_size)
            };
            assert!(!pageservers.is_empty());

            let ps_conf = env.get_pageserver_conf(DEFAULT_PAGESERVER_ID)?;
            let auth_token = if matches!(ps_conf.pg_auth_type, AuthType::NeonJWT) {
                let claims = Claims::new(Some(endpoint.tenant_id), Scope::Tenant);

                Some(env.generate_auth_token(&claims)?)
            } else {
                None
            };

            println!("Starting existing endpoint {endpoint_id}...");
            endpoint
                .start(
                    &auth_token,
                    safekeepers,
                    pageservers,
                    remote_ext_config,
                    stripe_size.0 as usize,
                    create_test_user,
                )
                .await?;
        }
        "reconfigure" => {
            let endpoint_id = sub_args
                .get_one::<String>("endpoint_id")
                .ok_or_else(|| anyhow!("No endpoint ID provided to reconfigure"))?;
            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;
            let pageservers =
                if let Some(id_str) = sub_args.get_one::<String>("endpoint-pageserver-id") {
                    let ps_id = NodeId(id_str.parse().context("while parsing pageserver id")?);
                    let pageserver = PageServerNode::from_env(env, env.get_pageserver_conf(ps_id)?);
                    vec![(
                        pageserver.pg_connection_config.host().clone(),
                        pageserver.pg_connection_config.port(),
                    )]
                } else {
                    let storage_controller = StorageController::from_env(env);
                    storage_controller
                        .tenant_locate(endpoint.tenant_id)
                        .await?
                        .shards
                        .into_iter()
                        .map(|shard| {
                            (
                                Host::parse(&shard.listen_pg_addr)
                                    .expect("Storage controller reported malformed host"),
                                shard.listen_pg_port,
                            )
                        })
                        .collect::<Vec<_>>()
                };
            endpoint.reconfigure(pageservers, None).await?;
        }
        "stop" => {
            let endpoint_id = sub_args
                .get_one::<String>("endpoint_id")
                .ok_or_else(|| anyhow!("No endpoint ID was provided to stop"))?;
            let destroy = sub_args.get_flag("destroy");
            let mode = sub_args.get_one::<String>("mode").expect("has a default");

            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;
            endpoint.stop(mode, destroy)?;
        }

        _ => bail!("Unexpected endpoint subcommand '{sub_name}'"),
    }

    Ok(())
}

fn handle_mappings(sub_match: &ArgMatches, env: &mut local_env::LocalEnv) -> Result<()> {
    let (sub_name, sub_args) = match sub_match.subcommand() {
        Some(ep_subcommand_data) => ep_subcommand_data,
        None => bail!("no mappings subcommand provided"),
    };

    match sub_name {
        "map" => {
            let branch_name = sub_args
                .get_one::<String>("branch-name")
                .expect("branch-name argument missing");

            let tenant_id = sub_args
                .get_one::<String>("tenant-id")
                .map(|x| TenantId::from_str(x))
                .expect("tenant-id argument missing")
                .expect("malformed tenant-id arg");

            let timeline_id = sub_args
                .get_one::<String>("timeline-id")
                .map(|x| TimelineId::from_str(x))
                .expect("timeline-id argument missing")
                .expect("malformed timeline-id arg");

            env.register_branch_mapping(branch_name.to_owned(), tenant_id, timeline_id)?;

            Ok(())
        }
        other => unimplemented!("mappings subcommand {other}"),
    }
}

fn get_pageserver(env: &local_env::LocalEnv, args: &ArgMatches) -> Result<PageServerNode> {
    let node_id = if let Some(id_str) = args.get_one::<String>("pageserver-id") {
        NodeId(id_str.parse().context("while parsing pageserver id")?)
    } else {
        DEFAULT_PAGESERVER_ID
    };

    Ok(PageServerNode::from_env(
        env,
        env.get_pageserver_conf(node_id)?,
    ))
}

fn get_start_timeout(args: &ArgMatches) -> &Duration {
    let humantime_duration = args
        .get_one::<humantime::Duration>("start-timeout")
        .expect("invalid value for start-timeout");
    humantime_duration.as_ref()
}

async fn handle_pageserver(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    match sub_match.subcommand() {
        Some(("start", subcommand_args)) => {
            if let Err(e) = get_pageserver(env, subcommand_args)?
                .start(get_start_timeout(subcommand_args))
                .await
            {
                eprintln!("pageserver start failed: {e}");
                exit(1);
            }
        }

        Some(("stop", subcommand_args)) => {
            let immediate = subcommand_args
                .get_one::<String>("stop-mode")
                .map(|s| s.as_str())
                == Some("immediate");

            if let Err(e) = get_pageserver(env, subcommand_args)?.stop(immediate) {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }
        }

        Some(("restart", subcommand_args)) => {
            let pageserver = get_pageserver(env, subcommand_args)?;
            //TODO what shutdown strategy should we use here?
            if let Err(e) = pageserver.stop(false) {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }

            if let Err(e) = pageserver.start(get_start_timeout(sub_match)).await {
                eprintln!("pageserver start failed: {e}");
                exit(1);
            }
        }

        Some(("status", subcommand_args)) => {
            match get_pageserver(env, subcommand_args)?.check_status().await {
                Ok(_) => println!("Page server is up and running"),
                Err(err) => {
                    eprintln!("Page server is not available: {}", err);
                    exit(1);
                }
            }
        }

        Some((sub_name, _)) => bail!("Unexpected pageserver subcommand '{}'", sub_name),
        None => bail!("no pageserver subcommand provided"),
    }
    Ok(())
}

async fn handle_storage_controller(
    sub_match: &ArgMatches,
    env: &local_env::LocalEnv,
) -> Result<()> {
    let svc = StorageController::from_env(env);
    match sub_match.subcommand() {
        Some(("start", start_match)) => {
            if let Err(e) = svc.start(get_start_timeout(start_match)).await {
                eprintln!("start failed: {e}");
                exit(1);
            }
        }

        Some(("stop", stop_match)) => {
            let immediate = stop_match
                .get_one::<String>("stop-mode")
                .map(|s| s.as_str())
                == Some("immediate");

            if let Err(e) = svc.stop(immediate).await {
                eprintln!("stop failed: {}", e);
                exit(1);
            }
        }
        Some((sub_name, _)) => bail!("Unexpected storage_controller subcommand '{}'", sub_name),
        None => bail!("no storage_controller subcommand provided"),
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

// Get list of options to append to safekeeper command invocation.
fn safekeeper_extra_opts(init_match: &ArgMatches) -> Vec<String> {
    init_match
        .get_many::<String>("safekeeper-extra-opt")
        .into_iter()
        .flatten()
        .map(|s| s.to_owned())
        .collect()
}

async fn handle_safekeeper(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let (sub_name, sub_args) = match sub_match.subcommand() {
        Some(safekeeper_command_data) => safekeeper_command_data,
        None => bail!("no safekeeper subcommand provided"),
    };

    // All the commands take an optional safekeeper name argument
    let sk_id = if let Some(id_str) = sub_args.get_one::<String>("id") {
        NodeId(id_str.parse().context("while parsing safekeeper id")?)
    } else {
        DEFAULT_SAFEKEEPER_ID
    };
    let safekeeper = get_safekeeper(env, sk_id)?;

    match sub_name {
        "start" => {
            let extra_opts = safekeeper_extra_opts(sub_args);

            if let Err(e) = safekeeper
                .start(extra_opts, get_start_timeout(sub_args))
                .await
            {
                eprintln!("safekeeper start failed: {}", e);
                exit(1);
            }
        }

        "stop" => {
            let immediate =
                sub_args.get_one::<String>("stop-mode").map(|s| s.as_str()) == Some("immediate");

            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {}", e);
                exit(1);
            }
        }

        "restart" => {
            let immediate =
                sub_args.get_one::<String>("stop-mode").map(|s| s.as_str()) == Some("immediate");

            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {}", e);
                exit(1);
            }

            let extra_opts = safekeeper_extra_opts(sub_args);
            if let Err(e) = safekeeper
                .start(extra_opts, get_start_timeout(sub_args))
                .await
            {
                eprintln!("safekeeper start failed: {}", e);
                exit(1);
            }
        }

        _ => {
            bail!("Unexpected safekeeper subcommand '{}'", sub_name)
        }
    }
    Ok(())
}

async fn handle_start_all(
    env: &local_env::LocalEnv,
    retry_timeout: &Duration,
) -> anyhow::Result<()> {
    // Endpoints are not started automatically

    broker::start_broker_process(env, retry_timeout).await?;

    // Only start the storage controller if the pageserver is configured to need it
    if env.control_plane_api.is_some() {
        let storage_controller = StorageController::from_env(env);
        if let Err(e) = storage_controller.start(retry_timeout).await {
            eprintln!("storage_controller start failed: {:#}", e);
            try_stop_all(env, true).await;
            exit(1);
        }
    }

    for ps_conf in &env.pageservers {
        let pageserver = PageServerNode::from_env(env, ps_conf);
        if let Err(e) = pageserver.start(retry_timeout).await {
            eprintln!("pageserver {} start failed: {:#}", ps_conf.id, e);
            try_stop_all(env, true).await;
            exit(1);
        }
    }

    for node in env.safekeepers.iter() {
        let safekeeper = SafekeeperNode::from_env(env, node);
        if let Err(e) = safekeeper.start(vec![], retry_timeout).await {
            eprintln!("safekeeper {} start failed: {:#}", safekeeper.id, e);
            try_stop_all(env, false).await;
            exit(1);
        }
    }
    Ok(())
}

async fn handle_stop_all(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let immediate =
        sub_match.get_one::<String>("stop-mode").map(|s| s.as_str()) == Some("immediate");

    try_stop_all(env, immediate).await;

    Ok(())
}

async fn try_stop_all(env: &local_env::LocalEnv, immediate: bool) {
    // Stop all endpoints
    match ComputeControlPlane::load(env.clone()) {
        Ok(cplane) => {
            for (_k, node) in cplane.endpoints {
                if let Err(e) = node.stop(if immediate { "immediate" } else { "fast" }, false) {
                    eprintln!("postgres stop failed: {e:#}");
                }
            }
        }
        Err(e) => {
            eprintln!("postgres stop failed, could not restore control plane data from env: {e:#}")
        }
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

    if let Err(e) = broker::stop_broker_process(env) {
        eprintln!("neon broker stop failed: {e:#}");
    }

    if env.control_plane_api.is_some() {
        let storage_controller = StorageController::from_env(env);
        if let Err(e) = storage_controller.stop(immediate).await {
            eprintln!("storage controller stop failed: {e:#}");
        }
    }
}

fn cli() -> Command {
    let timeout_arg = Arg::new("start-timeout")
        .long("start-timeout")
        .short('t')
        .global(true)
        .help("timeout until we fail the command, e.g. 30s")
        .value_parser(value_parser!(humantime::Duration))
        .default_value("10s")
        .required(false);

    let branch_name_arg = Arg::new("branch-name")
        .long("branch-name")
        .help("Name of the branch to be created or used as an alias for other services")
        .required(false);

    let endpoint_id_arg = Arg::new("endpoint_id")
        .help("Postgres endpoint id")
        .required(false);

    let safekeeper_id_arg = Arg::new("id").help("safekeeper id").required(false);

    // --id, when using a pageserver command
    let pageserver_id_arg = Arg::new("pageserver-id")
        .long("id")
        .global(true)
        .help("pageserver id")
        .required(false);
    // --pageserver-id when using a non-pageserver command
    let endpoint_pageserver_id_arg = Arg::new("endpoint-pageserver-id")
        .long("pageserver-id")
        .required(false);

    let safekeeper_extra_opt_arg = Arg::new("safekeeper-extra-opt")
        .short('e')
        .long("safekeeper-extra-opt")
        .num_args(1)
        .action(ArgAction::Append)
        .help("Additional safekeeper invocation options, e.g. -e=--http-auth-public-key-path=foo")
        .required(false);

    let tenant_id_arg = Arg::new("tenant-id")
        .long("tenant-id")
        .help("Tenant id. Represented as a hexadecimal string 32 symbols length")
        .required(false);

    let timeline_id_arg = Arg::new("timeline-id")
        .long("timeline-id")
        .help("Timeline id. Represented as a hexadecimal string 32 symbols length")
        .required(false);

    let pg_version_arg = Arg::new("pg-version")
        .long("pg-version")
        .help("Postgres version to use for the initial tenant")
        .required(false)
        .value_parser(value_parser!(u32))
        .default_value(DEFAULT_PG_VERSION);

    let pg_port_arg = Arg::new("pg-port")
        .long("pg-port")
        .required(false)
        .value_parser(value_parser!(u16))
        .value_name("pg-port");

    let http_port_arg = Arg::new("http-port")
        .long("http-port")
        .required(false)
        .value_parser(value_parser!(u16))
        .value_name("http-port");

    let safekeepers_arg = Arg::new("safekeepers")
        .long("safekeepers")
        .required(false)
        .value_name("safekeepers");

    let stop_mode_arg = Arg::new("stop-mode")
        .short('m')
        .value_parser(["fast", "immediate"])
        .default_value("fast")
        .help("If 'immediate', don't flush repository data at shutdown")
        .required(false)
        .value_name("stop-mode");

    let remote_ext_config_args = Arg::new("remote-ext-config")
        .long("remote-ext-config")
        .num_args(1)
        .help("Configure the remote extensions storage proxy gateway to request for extensions.")
        .required(false);

    let lsn_arg = Arg::new("lsn")
        .long("lsn")
        .help("Specify Lsn on the timeline to start from. By default, end of the timeline would be used.")
        .required(false);

    let hot_standby_arg = Arg::new("hot-standby")
        .value_parser(value_parser!(bool))
        .long("hot-standby")
        .help("If set, the node will be a hot replica on the specified timeline")
        .required(false);

    let force_arg = Arg::new("force")
        .value_parser(value_parser!(InitForceMode))
        .long("force")
        .default_value(
            InitForceMode::MustNotExist
                .to_possible_value()
                .unwrap()
                .get_name()
                .to_owned(),
        )
        .help("Force initialization even if the repository is not empty")
        .required(false);

    let num_pageservers_arg = Arg::new("num-pageservers")
        .value_parser(value_parser!(u16))
        .long("num-pageservers")
        .help("How many pageservers to create (default 1)");

    let update_catalog = Arg::new("update-catalog")
        .value_parser(value_parser!(bool))
        .long("update-catalog")
        .help("If set, will set up the catalog for neon_superuser")
        .required(false);

    let create_test_user = Arg::new("create-test-user")
        .value_parser(value_parser!(bool))
        .long("create-test-user")
        .help("If set, will create test user `user` and `neondb` database. Requires `update-catalog = true`")
        .required(false);

    let allow_multiple = Arg::new("allow-multiple")
        .help("Allow multiple primary endpoints running on the same branch. Shouldn't be used normally, but useful for tests.")
        .long("allow-multiple")
        .action(ArgAction::SetTrue)
        .required(false);

    Command::new("Neon CLI")
        .arg_required_else_help(true)
        .version(GIT_VERSION)
        .subcommand(
            Command::new("init")
                .about("Initialize a new Neon repository, preparing configs for services to start with")
                .arg(num_pageservers_arg.clone())
                .arg(
                    Arg::new("config")
                        .long("config")
                        .required(false)
                        .value_parser(value_parser!(PathBuf))
                        .value_name("config")
                )
                .arg(pg_version_arg.clone())
                .arg(force_arg)
        )
        .subcommand(
            Command::new("timeline")
            .about("Manage timelines")
            .arg_required_else_help(true)
            .subcommand(Command::new("list")
                .about("List all timelines, available to this pageserver")
                .arg(tenant_id_arg.clone()))
            .subcommand(Command::new("branch")
                .about("Create a new timeline, using another timeline as a base, copying its data")
                .arg(tenant_id_arg.clone())
                .arg(branch_name_arg.clone())
                .arg(Arg::new("ancestor-branch-name").long("ancestor-branch-name")
                    .help("Use last Lsn of another timeline (and its data) as base when creating the new timeline. The timeline gets resolved by its branch name.").required(false))
                .arg(Arg::new("ancestor-start-lsn").long("ancestor-start-lsn")
                    .help("When using another timeline as base, use a specific Lsn in it instead of the latest one").required(false)))
            .subcommand(Command::new("create")
                .about("Create a new blank timeline")
                .arg(tenant_id_arg.clone())
                .arg(timeline_id_arg.clone())
                .arg(branch_name_arg.clone())
                .arg(pg_version_arg.clone())
            )
            .subcommand(Command::new("import")
                .about("Import timeline from basebackup directory")
                .arg(tenant_id_arg.clone())
                .arg(timeline_id_arg.clone())
                .arg(branch_name_arg.clone())
                .arg(Arg::new("base-tarfile")
                    .long("base-tarfile")
                    .value_parser(value_parser!(PathBuf))
                    .help("Basebackup tarfile to import")
                )
                .arg(Arg::new("base-lsn").long("base-lsn")
                    .help("Lsn the basebackup starts at"))
                .arg(Arg::new("wal-tarfile")
                    .long("wal-tarfile")
                    .value_parser(value_parser!(PathBuf))
                    .help("Wal to add after base")
                )
                .arg(Arg::new("end-lsn").long("end-lsn")
                    .help("Lsn the basebackup ends at"))
                .arg(pg_version_arg.clone())
            )
        ).subcommand(
            Command::new("tenant")
            .arg_required_else_help(true)
            .about("Manage tenants")
            .subcommand(Command::new("list"))
            .subcommand(Command::new("create")
                .arg(tenant_id_arg.clone())
                .arg(timeline_id_arg.clone().help("Use a specific timeline id when creating a tenant and its initial timeline"))
                .arg(Arg::new("config").short('c').num_args(1).action(ArgAction::Append).required(false))
                .arg(pg_version_arg.clone())
                .arg(Arg::new("set-default").long("set-default").action(ArgAction::SetTrue).required(false)
                    .help("Use this tenant in future CLI commands where tenant_id is needed, but not specified"))
                .arg(Arg::new("shard-count").value_parser(value_parser!(u8)).long("shard-count").action(ArgAction::Set).help("Number of shards in the new tenant (default 1)"))
                .arg(Arg::new("shard-stripe-size").value_parser(value_parser!(u32)).long("shard-stripe-size").action(ArgAction::Set).help("Sharding stripe size in pages"))
                .arg(Arg::new("placement-policy").value_parser(value_parser!(String)).long("placement-policy").action(ArgAction::Set).help("Placement policy shards in this tenant"))
                )
            .subcommand(Command::new("set-default").arg(tenant_id_arg.clone().required(true))
                .about("Set a particular tenant as default in future CLI commands where tenant_id is needed, but not specified"))
            .subcommand(Command::new("config")
                .arg(tenant_id_arg.clone())
                .arg(Arg::new("config").short('c').num_args(1).action(ArgAction::Append).required(false)))
            .subcommand(Command::new("import").arg(tenant_id_arg.clone().required(true))
                .about("Import a tenant that is present in remote storage, and create branches for its timelines"))
        )
        .subcommand(
            Command::new("pageserver")
                .arg_required_else_help(true)
                .about("Manage pageserver")
                .arg(pageserver_id_arg)
                .subcommand(Command::new("status"))
                .subcommand(Command::new("start")
                    .about("Start local pageserver")
                    .arg(timeout_arg.clone())
                )
                .subcommand(Command::new("stop")
                    .about("Stop local pageserver")
                    .arg(stop_mode_arg.clone())
                )
                .subcommand(Command::new("restart")
                    .about("Restart local pageserver")
                    .arg(timeout_arg.clone())
                )
        )
        .subcommand(
            Command::new("storage_controller")
                .arg_required_else_help(true)
                .about("Manage storage_controller")
                .subcommand(Command::new("start").about("Start storage controller")
                            .arg(timeout_arg.clone()))
                .subcommand(Command::new("stop").about("Stop storage controller")
                            .arg(stop_mode_arg.clone()))
        )
        .subcommand(
            Command::new("safekeeper")
                .arg_required_else_help(true)
                .about("Manage safekeepers")
                .subcommand(Command::new("start")
                            .about("Start local safekeeper")
                            .arg(safekeeper_id_arg.clone())
                            .arg(safekeeper_extra_opt_arg.clone())
                            .arg(timeout_arg.clone())
                )
                .subcommand(Command::new("stop")
                            .about("Stop local safekeeper")
                            .arg(safekeeper_id_arg.clone())
                            .arg(stop_mode_arg.clone())
                )
                .subcommand(Command::new("restart")
                            .about("Restart local safekeeper")
                            .arg(safekeeper_id_arg)
                            .arg(stop_mode_arg.clone())
                            .arg(safekeeper_extra_opt_arg)
                            .arg(timeout_arg.clone())
                )
        )
        .subcommand(
            Command::new("endpoint")
                .arg_required_else_help(true)
                .about("Manage postgres instances")
                .subcommand(Command::new("list").arg(tenant_id_arg.clone()))
                .subcommand(Command::new("create")
                    .about("Create a compute endpoint")
                    .arg(endpoint_id_arg.clone())
                    .arg(branch_name_arg.clone())
                    .arg(tenant_id_arg.clone())
                    .arg(lsn_arg.clone())
                    .arg(pg_port_arg.clone())
                    .arg(http_port_arg.clone())
                    .arg(endpoint_pageserver_id_arg.clone())
                    .arg(
                        Arg::new("config-only")
                            .help("Don't do basebackup, create endpoint directory with only config files")
                            .long("config-only")
                            .required(false))
                    .arg(pg_version_arg.clone())
                    .arg(hot_standby_arg.clone())
                    .arg(update_catalog)
                    .arg(allow_multiple.clone())
                )
                .subcommand(Command::new("start")
                    .about("Start postgres.\n If the endpoint doesn't exist yet, it is created.")
                    .arg(endpoint_id_arg.clone())
                    .arg(endpoint_pageserver_id_arg.clone())
                    .arg(safekeepers_arg)
                    .arg(remote_ext_config_args)
                    .arg(create_test_user)
                    .arg(allow_multiple.clone())
                    .arg(timeout_arg.clone())
                )
                .subcommand(Command::new("reconfigure")
                            .about("Reconfigure the endpoint")
                            .arg(endpoint_pageserver_id_arg)
                            .arg(endpoint_id_arg.clone())
                            .arg(tenant_id_arg.clone())
                )
                .subcommand(
                    Command::new("stop")
                    .arg(endpoint_id_arg)
                    .arg(
                        Arg::new("destroy")
                            .help("Also delete data directory (now optional, should be default in future)")
                            .long("destroy")
                            .action(ArgAction::SetTrue)
                            .required(false)
                    )
                    .arg(
                        Arg::new("mode")
                            .help("Postgres shutdown mode, passed to \"pg_ctl -m <mode>\"")
                            .long("mode")
                            .action(ArgAction::Set)
                            .required(false)
                            .value_parser(["smart", "fast", "immediate"])
                            .default_value("fast")
                    )
                )

        )
        .subcommand(
            Command::new("mappings")
                .arg_required_else_help(true)
                .about("Manage neon_local branch name mappings")
                .subcommand(
                    Command::new("map")
                        .about("Create new mapping which cannot exist already")
                        .arg(branch_name_arg.clone())
                        .arg(tenant_id_arg.clone())
                        .arg(timeline_id_arg.clone())
                )
        )
        // Obsolete old name for 'endpoint'. We now just print an error if it's used.
        .subcommand(
            Command::new("pg")
                .hide(true)
                .arg(Arg::new("ignore-rest").allow_hyphen_values(true).num_args(0..).required(false))
                .trailing_var_arg(true)
        )
        .subcommand(
            Command::new("start")
                .about("Start page server and safekeepers")
                .arg(timeout_arg.clone())
        )
        .subcommand(
            Command::new("stop")
                .about("Stop page server and safekeepers")
                .arg(stop_mode_arg)
        )
}

#[test]
fn verify_cli() {
    cli().debug_assert();
}
