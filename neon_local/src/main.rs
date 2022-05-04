use anyhow::{anyhow, bail, Context, Result};
use clap::{App, AppSettings, Arg, ArgMatches};
use control_plane::compute::ComputeControlPlane;
use control_plane::local_env;
use control_plane::local_env::LocalEnv;
use control_plane::safekeeper::SafekeeperNode;
use control_plane::storage::PageServerNode;
use pageserver::config::defaults::{
    DEFAULT_HTTP_LISTEN_ADDR as DEFAULT_PAGESERVER_HTTP_ADDR,
    DEFAULT_PG_LISTEN_ADDR as DEFAULT_PAGESERVER_PG_ADDR,
};
use safekeeper::defaults::{
    DEFAULT_HTTP_LISTEN_PORT as DEFAULT_SAFEKEEPER_HTTP_PORT,
    DEFAULT_PG_LISTEN_PORT as DEFAULT_SAFEKEEPER_PG_PORT,
};
use std::collections::{BTreeSet, HashMap};
use std::process::exit;
use std::str::FromStr;
use utils::{
    auth::{Claims, Scope},
    lsn::Lsn,
    postgres_backend::AuthType,
    zid::{ZNodeId, ZTenantId, ZTenantTimelineId, ZTimelineId},
    GIT_VERSION,
};

use pageserver::timelines::TimelineInfo;

// Default id of a safekeeper node, if not specified on the command line.
const DEFAULT_SAFEKEEPER_ID: ZNodeId = ZNodeId(1);
const DEFAULT_PAGESERVER_ID: ZNodeId = ZNodeId(1);
const DEFAULT_BRANCH_NAME: &str = "main";

fn default_conf() -> String {
    format!(
        r#"
# Default built-in configuration, defined in main.rs
[pageserver]
id = {pageserver_id}
listen_pg_addr = '{pageserver_pg_addr}'
listen_http_addr = '{pageserver_http_addr}'
auth_type = '{pageserver_auth_type}'

[[safekeepers]]
id = {safekeeper_id}
pg_port = {safekeeper_pg_port}
http_port = {safekeeper_http_port}
"#,
        pageserver_id = DEFAULT_PAGESERVER_ID,
        pageserver_pg_addr = DEFAULT_PAGESERVER_PG_ADDR,
        pageserver_http_addr = DEFAULT_PAGESERVER_HTTP_ADDR,
        pageserver_auth_type = AuthType::Trust,
        safekeeper_id = DEFAULT_SAFEKEEPER_ID,
        safekeeper_pg_port = DEFAULT_SAFEKEEPER_PG_PORT,
        safekeeper_http_port = DEFAULT_SAFEKEEPER_HTTP_PORT,
    )
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
    pub children: BTreeSet<ZTimelineId>,
}

// Main entry point for the 'neon_local' CLI utility
//
// This utility helps to manage neon installation. That includes following:
//   * Management of local postgres installations running on top of the
//     pageserver.
//   * Providing CLI api to the pageserver
//   * TODO: export/import to/from usual postgres
fn main() -> Result<()> {
    let branch_name_arg = Arg::new("branch-name")
        .long("branch-name")
        .takes_value(true)
        .help("Name of the branch to be created or used as an alias for other services")
        .required(false);

    let pg_node_arg = Arg::new("node").help("Postgres node name").required(false);

    let safekeeper_id_arg = Arg::new("id").help("safekeeper id").required(false);

    let tenant_id_arg = Arg::new("tenant-id")
        .long("tenant-id")
        .help("Tenant id. Represented as a hexadecimal string 32 symbols length")
        .takes_value(true)
        .required(false);

    let timeline_id_arg = Arg::new("timeline-id")
        .long("timeline-id")
        .help("Timeline id. Represented as a hexadecimal string 32 symbols length")
        .takes_value(true)
        .required(false);

    let port_arg = Arg::new("port")
        .long("port")
        .required(false)
        .value_name("port");

    let stop_mode_arg = Arg::new("stop-mode")
        .short('m')
        .takes_value(true)
        .possible_values(&["fast", "immediate"])
        .help("If 'immediate', don't flush repository data at shutdown")
        .required(false)
        .value_name("stop-mode");

    let pageserver_config_args = Arg::new("pageserver-config-override")
        .long("pageserver-config-override")
        .takes_value(true)
        .number_of_values(1)
        .multiple_occurrences(true)
        .help("Additional pageserver's configuration options or overrides, refer to pageserver's 'config-override' CLI parameter docs for more")
        .required(false);

    let lsn_arg = Arg::new("lsn")
        .long("lsn")
        .help("Specify Lsn on the timeline to start from. By default, end of the timeline would be used.")
        .takes_value(true)
        .required(false);

    let matches = App::new("Neon CLI")
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(GIT_VERSION)
        .subcommand(
            App::new("init")
                .about("Initialize a new Neon repository")
                .arg(pageserver_config_args.clone())
                .arg(timeline_id_arg.clone().help("Use a specific timeline id when creating a tenant and its initial timeline"))
                .arg(
                    Arg::new("config")
                        .long("config")
                        .required(false)
                        .value_name("config"),
                )
        )
        .subcommand(
            App::new("timeline")
            .about("Manage timelines")
            .subcommand(App::new("list")
                .about("List all timelines, available to this pageserver")
                .arg(tenant_id_arg.clone()))
            .subcommand(App::new("branch")
                .about("Create a new timeline, using another timeline as a base, copying its data")
                .arg(tenant_id_arg.clone())
                .arg(branch_name_arg.clone())
                .arg(Arg::new("ancestor-branch-name").long("ancestor-branch-name").takes_value(true)
                    .help("Use last Lsn of another timeline (and its data) as base when creating the new timeline. The timeline gets resolved by its branch name.").required(false))
                .arg(Arg::new("ancestor-start-lsn").long("ancestor-start-lsn").takes_value(true)
                    .help("When using another timeline as base, use a specific Lsn in it instead of the latest one").required(false)))
            .subcommand(App::new("create")
                .about("Create a new blank timeline")
                .arg(tenant_id_arg.clone())
                .arg(branch_name_arg.clone()))
        ).subcommand(
            App::new("tenant")
            .setting(AppSettings::ArgRequiredElseHelp)
            .about("Manage tenants")
            .subcommand(App::new("list"))
            .subcommand(App::new("create")
                .arg(tenant_id_arg.clone())
                .arg(timeline_id_arg.clone().help("Use a specific timeline id when creating a tenant and its initial timeline"))
				.arg(Arg::new("config").short('c').takes_value(true).multiple_occurrences(true).required(false))
				)
            .subcommand(App::new("config")
                .arg(tenant_id_arg.clone())
				.arg(Arg::new("config").short('c').takes_value(true).multiple_occurrences(true).required(false))
				)
        )
        .subcommand(
            App::new("pageserver")
                .setting(AppSettings::ArgRequiredElseHelp)
                .about("Manage pageserver")
                .subcommand(App::new("status"))
                .subcommand(App::new("start").about("Start local pageserver").arg(pageserver_config_args.clone()))
                .subcommand(App::new("stop").about("Stop local pageserver")
                            .arg(stop_mode_arg.clone()))
                .subcommand(App::new("restart").about("Restart local pageserver").arg(pageserver_config_args.clone()))
        )
        .subcommand(
            App::new("safekeeper")
                .setting(AppSettings::ArgRequiredElseHelp)
                .about("Manage safekeepers")
                .subcommand(App::new("start")
                            .about("Start local safekeeper")
                            .arg(safekeeper_id_arg.clone())
                )
                .subcommand(App::new("stop")
                            .about("Stop local safekeeper")
                            .arg(safekeeper_id_arg.clone())
                            .arg(stop_mode_arg.clone())
                )
                .subcommand(App::new("restart")
                            .about("Restart local safekeeper")
                            .arg(safekeeper_id_arg.clone())
                            .arg(stop_mode_arg.clone())
                )
        )
        .subcommand(
            App::new("pg")
                .setting(AppSettings::ArgRequiredElseHelp)
                .about("Manage postgres instances")
                .subcommand(App::new("list").arg(tenant_id_arg.clone()))
                .subcommand(App::new("create")
                    .about("Create a postgres compute node")
                    .arg(pg_node_arg.clone())
                    .arg(branch_name_arg.clone())
                    .arg(tenant_id_arg.clone())
                    .arg(lsn_arg.clone())
                    .arg(port_arg.clone())
                    .arg(
                        Arg::new("config-only")
                            .help("Don't do basebackup, create compute node with only config files")
                            .long("config-only")
                            .required(false)
                    ))
                .subcommand(App::new("start")
                    .about("Start a postgres compute node.\n This command actually creates new node from scratch, but preserves existing config files")
                    .arg(pg_node_arg.clone())
                    .arg(tenant_id_arg.clone())
                    .arg(branch_name_arg.clone())
                    .arg(timeline_id_arg.clone())
                    .arg(lsn_arg.clone())
                    .arg(port_arg.clone()))
                .subcommand(
                    App::new("stop")
                    .arg(pg_node_arg.clone())
                    .arg(tenant_id_arg.clone())
                    .arg(
                        Arg::new("destroy")
                            .help("Also delete data directory (now optional, should be default in future)")
                            .long("destroy")
                            .required(false)
                    )
                    )

        )
        .subcommand(
            App::new("start")
                .about("Start page server and safekeepers")
                .arg(pageserver_config_args)
        )
        .subcommand(
            App::new("stop")
                .about("Stop page server and safekeepers")
                .arg(stop_mode_arg.clone())
        )
        .get_matches();

    let (sub_name, sub_args) = match matches.subcommand() {
        Some(subcommand_data) => subcommand_data,
        None => bail!("no subcommand provided"),
    };

    // Check for 'neon init' command first.
    let subcommand_result = if sub_name == "init" {
        handle_init(sub_args).map(Some)
    } else {
        // all other commands need an existing config
        let mut env = LocalEnv::load_config().context("Error loading config")?;
        let original_env = env.clone();

        let subcommand_result = match sub_name {
            "tenant" => handle_tenant(sub_args, &mut env),
            "timeline" => handle_timeline(sub_args, &mut env),
            "start" => handle_start_all(sub_args, &env),
            "stop" => handle_stop_all(sub_args, &env),
            "pageserver" => handle_pageserver(sub_args, &env),
            "pg" => handle_pg(sub_args, &env),
            "safekeeper" => handle_safekeeper(sub_args, &env),
            _ => bail!("unexpected subcommand {}", sub_name),
        };

        if original_env != env {
            subcommand_result.map(|()| Some(env))
        } else {
            subcommand_result.map(|()| None)
        }
    };

    match subcommand_result {
        Ok(Some(updated_env)) => updated_env.persist_config(&updated_env.base_data_dir)?,
        Ok(None) => (),
        Err(e) => {
            eprintln!("command failed: {:?}", e);
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
    mut timeline_name_mappings: HashMap<ZTenantTimelineId, String>,
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
                        .remove(&ZTenantTimelineId::new(t.tenant_id, t.timeline_id)),
                },
            )
        })
        .collect::<HashMap<_, _>>();

    // Memorize all direct children of each timeline.
    for timeline in timelines.iter() {
        if let Some(ancestor_timeline_id) =
            timeline.local.as_ref().and_then(|l| l.ancestor_timeline_id)
        {
            timelines_hash
                .get_mut(&ancestor_timeline_id)
                .context("missing timeline info in the HashMap")?
                .children
                .insert(timeline.timeline_id);
        }
    }

    for timeline in timelines_hash.values() {
        // Start with root local timelines (no ancestors) first.
        if timeline
            .info
            .local
            .as_ref()
            .and_then(|l| l.ancestor_timeline_id)
            .is_none()
        {
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
    timelines: &HashMap<ZTimelineId, TimelineTreeEl>,
) -> Result<()> {
    let local_remote = match (timeline.info.local.as_ref(), timeline.info.remote.as_ref()) {
        (None, None) => unreachable!("in this case no info for a timeline is found"),
        (None, Some(_)) => "(R)",
        (Some(_), None) => "(L)",
        (Some(_), Some(_)) => "(L+R)",
    };
    // Draw main padding
    print!("{} ", local_remote);

    if nesting_level > 0 {
        let ancestor_lsn = match timeline.info.local.as_ref().and_then(|i| i.ancestor_lsn) {
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
fn get_timeline_infos(
    env: &local_env::LocalEnv,
    tenant_id: &ZTenantId,
) -> Result<HashMap<ZTimelineId, TimelineInfo>> {
    Ok(PageServerNode::from_env(env)
        .timeline_list(tenant_id)?
        .into_iter()
        .map(|timeline_info| (timeline_info.timeline_id, timeline_info))
        .collect())
}

// Helper function to parse --tenant_id option, or get the default from config file
fn get_tenant_id(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> anyhow::Result<ZTenantId> {
    if let Some(tenant_id_from_arguments) = parse_tenant_id(sub_match).transpose() {
        tenant_id_from_arguments
    } else if let Some(default_id) = env.default_tenant_id {
        Ok(default_id)
    } else {
        bail!("No tenant id. Use --tenant-id, or set 'default_tenant_id' in the config file");
    }
}

fn parse_tenant_id(sub_match: &ArgMatches) -> anyhow::Result<Option<ZTenantId>> {
    sub_match
        .value_of("tenant-id")
        .map(ZTenantId::from_str)
        .transpose()
        .context("Failed to parse tenant id from the argument string")
}

fn parse_timeline_id(sub_match: &ArgMatches) -> anyhow::Result<Option<ZTimelineId>> {
    sub_match
        .value_of("timeline-id")
        .map(ZTimelineId::from_str)
        .transpose()
        .context("Failed to parse timeline id from the argument string")
}

fn handle_init(init_match: &ArgMatches) -> Result<LocalEnv> {
    let initial_timeline_id_arg = parse_timeline_id(init_match)?;

    // Create config file
    let toml_file: String = if let Some(config_path) = init_match.value_of("config") {
        // load and parse the file
        std::fs::read_to_string(std::path::Path::new(config_path))
            .with_context(|| format!("Could not read configuration file \"{}\"", config_path))?
    } else {
        // Built-in default config
        default_conf()
    };

    let mut env =
        LocalEnv::create_config(&toml_file).context("Failed to create neon configuration")?;
    env.init().context("Failed to initialize neon repository")?;

    // default_tenantid was generated by the `env.init()` call above
    let initial_tenant_id = env.default_tenant_id.unwrap();

    // Call 'pageserver init'.
    let pageserver = PageServerNode::from_env(&env);
    let initial_timeline_id = pageserver
        .init(
            Some(initial_tenant_id),
            initial_timeline_id_arg,
            &pageserver_config_overrides(init_match),
        )
        .unwrap_or_else(|e| {
            eprintln!("pageserver init failed: {}", e);
            exit(1);
        });

    env.register_branch_mapping(
        DEFAULT_BRANCH_NAME.to_owned(),
        initial_tenant_id,
        initial_timeline_id,
    )?;

    Ok(env)
}

fn pageserver_config_overrides(init_match: &ArgMatches) -> Vec<&str> {
    init_match
        .values_of("pageserver-config-override")
        .into_iter()
        .flatten()
        .collect()
}

fn handle_tenant(tenant_match: &ArgMatches, env: &mut local_env::LocalEnv) -> Result<()> {
    let pageserver = PageServerNode::from_env(env);
    match tenant_match.subcommand() {
        Some(("list", _)) => {
            for t in pageserver.tenant_list()? {
                println!("{} {}", t.id, t.state);
            }
        }
        Some(("create", create_match)) => {
            let initial_tenant_id = parse_tenant_id(create_match)?;
            let tenant_conf: HashMap<_, _> = create_match
                .values_of("config")
                .map(|vals| vals.flat_map(|c| c.split_once(':')).collect())
                .unwrap_or_default();
            let new_tenant_id = pageserver
                .tenant_create(initial_tenant_id, tenant_conf)?
                .ok_or_else(|| {
                    anyhow!("Tenant with id {:?} was already created", initial_tenant_id)
                })?;
            println!(
                "tenant {} successfully created on the pageserver",
                new_tenant_id
            );
        }
        Some(("config", create_match)) => {
            let tenant_id = get_tenant_id(create_match, env)?;
            let tenant_conf: HashMap<_, _> = create_match
                .values_of("config")
                .map(|vals| vals.flat_map(|c| c.split_once(':')).collect())
                .unwrap_or_default();

            pageserver
                .tenant_config(tenant_id, tenant_conf)
                .unwrap_or_else(|e| {
                    anyhow!(
                        "Tenant config failed for tenant with id {} : {}",
                        tenant_id,
                        e
                    );
                });
            println!(
                "tenant {} successfully configured on the pageserver",
                tenant_id
            );
        }
        Some((sub_name, _)) => bail!("Unexpected tenant subcommand '{}'", sub_name),
        None => bail!("no tenant subcommand provided"),
    }
    Ok(())
}

fn handle_timeline(timeline_match: &ArgMatches, env: &mut local_env::LocalEnv) -> Result<()> {
    let pageserver = PageServerNode::from_env(env);

    match timeline_match.subcommand() {
        Some(("list", list_match)) => {
            let tenant_id = get_tenant_id(list_match, env)?;
            let timelines = pageserver.timeline_list(&tenant_id)?;
            print_timelines_tree(timelines, env.timeline_name_mappings())?;
        }
        Some(("create", create_match)) => {
            let tenant_id = get_tenant_id(create_match, env)?;
            let new_branch_name = create_match
                .value_of("branch-name")
                .ok_or_else(|| anyhow!("No branch name provided"))?;
            let timeline = pageserver
                .timeline_create(tenant_id, None, None, None)?
                .ok_or_else(|| anyhow!("Failed to create new timeline for tenant {}", tenant_id))?;
            let new_timeline_id = timeline.timeline_id;

            let last_record_lsn = timeline
                .local
                .expect("no local timeline info")
                .last_record_lsn;
            env.register_branch_mapping(new_branch_name.to_string(), tenant_id, new_timeline_id)?;

            println!(
                "Created timeline '{}' at Lsn {} for tenant: {}",
                timeline.timeline_id, last_record_lsn, tenant_id,
            );
        }
        Some(("branch", branch_match)) => {
            let tenant_id = get_tenant_id(branch_match, env)?;
            let new_branch_name = branch_match
                .value_of("branch-name")
                .ok_or_else(|| anyhow!("No branch name provided"))?;
            let ancestor_branch_name = branch_match
                .value_of("ancestor-branch-name")
                .unwrap_or(DEFAULT_BRANCH_NAME);
            let ancestor_timeline_id = env
                .get_branch_timeline_id(ancestor_branch_name, tenant_id)
                .ok_or_else(|| {
                    anyhow!(
                        "Found no timeline id for branch name '{}'",
                        ancestor_branch_name
                    )
                })?;

            let start_lsn = branch_match
                .value_of("ancestor-start-lsn")
                .map(Lsn::from_str)
                .transpose()
                .context("Failed to parse ancestor start Lsn from the request")?;
            let timeline = pageserver
                .timeline_create(tenant_id, None, start_lsn, Some(ancestor_timeline_id))?
                .ok_or_else(|| anyhow!("Failed to create new timeline for tenant {}", tenant_id))?;
            let new_timeline_id = timeline.timeline_id;

            let last_record_lsn = timeline
                .local
                .expect("no local timeline info")
                .last_record_lsn;

            env.register_branch_mapping(new_branch_name.to_string(), tenant_id, new_timeline_id)?;

            println!(
                "Created timeline '{}' at Lsn {} for tenant: {}. Ancestor timeline: '{}'",
                timeline.timeline_id, last_record_lsn, tenant_id, ancestor_branch_name,
            );
        }
        Some((sub_name, _)) => bail!("Unexpected tenant subcommand '{}'", sub_name),
        None => bail!("no tenant subcommand provided"),
    }

    Ok(())
}

fn handle_pg(pg_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let (sub_name, sub_args) = match pg_match.subcommand() {
        Some(pg_subcommand_data) => pg_subcommand_data,
        None => bail!("no pg subcommand provided"),
    };

    let mut cplane = ComputeControlPlane::load(env.clone())?;

    // All subcommands take an optional --tenant-id option
    let tenant_id = get_tenant_id(sub_args, env)?;

    match sub_name {
        "list" => {
            let timeline_infos = get_timeline_infos(env, &tenant_id).unwrap_or_else(|e| {
                eprintln!("Failed to load timeline info: {}", e);
                HashMap::new()
            });

            let timeline_name_mappings = env.timeline_name_mappings();

            let mut table = comfy_table::Table::new();

            table.load_preset(comfy_table::presets::NOTHING);

            table.set_header(&[
                "NODE",
                "ADDRESS",
                "TIMELINE",
                "BRANCH NAME",
                "LSN",
                "STATUS",
            ]);

            for ((_, node_name), node) in cplane
                .nodes
                .iter()
                .filter(|((node_tenant_id, _), _)| node_tenant_id == &tenant_id)
            {
                // FIXME: This shows the LSN at the end of the timeline. It's not the
                // right thing to do for read-only nodes that might be anchored at an
                // older point in time, or following but lagging behind the primary.
                let lsn_str = timeline_infos
                    .get(&node.timeline_id)
                    .and_then(|bi| bi.local.as_ref().map(|l| l.last_record_lsn.to_string()))
                    .unwrap_or_else(|| "?".to_string());

                let branch_name = timeline_name_mappings
                    .get(&ZTenantTimelineId::new(tenant_id, node.timeline_id))
                    .map(|name| name.as_str())
                    .unwrap_or("?");

                table.add_row(&[
                    node_name.as_str(),
                    &node.address.to_string(),
                    &node.timeline_id.to_string(),
                    branch_name,
                    lsn_str.as_str(),
                    node.status(),
                ]);
            }

            println!("{table}");
        }
        "create" => {
            let branch_name = sub_args
                .value_of("branch-name")
                .unwrap_or(DEFAULT_BRANCH_NAME);
            let node_name = sub_args
                .value_of("node")
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("{}_node", branch_name));

            let lsn = sub_args
                .value_of("lsn")
                .map(Lsn::from_str)
                .transpose()
                .context("Failed to parse Lsn from the request")?;
            let timeline_id = env
                .get_branch_timeline_id(branch_name, tenant_id)
                .ok_or_else(|| anyhow!("Found no timeline id for branch name '{}'", branch_name))?;

            let port: Option<u16> = match sub_args.value_of("port") {
                Some(p) => Some(p.parse()?),
                None => None,
            };
            cplane.new_node(tenant_id, &node_name, timeline_id, lsn, port)?;
        }
        "start" => {
            let port: Option<u16> = match sub_args.value_of("port") {
                Some(p) => Some(p.parse()?),
                None => None,
            };
            let node_name = sub_args
                .value_of("node")
                .ok_or_else(|| anyhow!("No node name was provided to start"))?;

            let node = cplane.nodes.get(&(tenant_id, node_name.to_owned()));

            let auth_token = if matches!(env.pageserver.auth_type, AuthType::ZenithJWT) {
                let claims = Claims::new(Some(tenant_id), Scope::Tenant);

                Some(env.generate_auth_token(&claims)?)
            } else {
                None
            };

            if let Some(node) = node {
                println!("Starting existing postgres {}...", node_name);
                node.start(&auth_token)?;
            } else {
                let branch_name = sub_args
                    .value_of("branch-name")
                    .unwrap_or(DEFAULT_BRANCH_NAME);
                let timeline_id = env
                    .get_branch_timeline_id(branch_name, tenant_id)
                    .ok_or_else(|| {
                        anyhow!("Found no timeline id for branch name '{}'", branch_name)
                    })?;
                let lsn = sub_args
                    .value_of("lsn")
                    .map(Lsn::from_str)
                    .transpose()
                    .context("Failed to parse Lsn from the request")?;
                // when used with custom port this results in non obvious behaviour
                // port is remembered from first start command, i e
                // start --port X
                // stop
                // start <-- will also use port X even without explicit port argument
                println!(
                    "Starting new postgres {} on timeline {} ...",
                    node_name, timeline_id
                );
                let node = cplane.new_node(tenant_id, node_name, timeline_id, lsn, port)?;
                node.start(&auth_token)?;
            }
        }
        "stop" => {
            let node_name = sub_args
                .value_of("node")
                .ok_or_else(|| anyhow!("No node name was provided to stop"))?;
            let destroy = sub_args.is_present("destroy");

            let node = cplane
                .nodes
                .get(&(tenant_id, node_name.to_owned()))
                .with_context(|| format!("postgres {} is not found", node_name))?;
            node.stop(destroy)?;
        }

        _ => bail!("Unexpected pg subcommand '{}'", sub_name),
    }

    Ok(())
}

fn handle_pageserver(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let pageserver = PageServerNode::from_env(env);

    match sub_match.subcommand() {
        Some(("start", start_match)) => {
            if let Err(e) = pageserver.start(&pageserver_config_overrides(start_match)) {
                eprintln!("pageserver start failed: {}", e);
                exit(1);
            }
        }

        Some(("stop", stop_match)) => {
            let immediate = stop_match.value_of("stop-mode") == Some("immediate");

            if let Err(e) = pageserver.stop(immediate) {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }
        }

        Some(("restart", restart_match)) => {
            //TODO what shutdown strategy should we use here?
            if let Err(e) = pageserver.stop(false) {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }

            if let Err(e) = pageserver.start(&pageserver_config_overrides(restart_match)) {
                eprintln!("pageserver start failed: {}", e);
                exit(1);
            }
        }
        Some((sub_name, _)) => bail!("Unexpected pageserver subcommand '{}'", sub_name),
        None => bail!("no pageserver subcommand provided"),
    }
    Ok(())
}

fn get_safekeeper(env: &local_env::LocalEnv, id: ZNodeId) -> Result<SafekeeperNode> {
    if let Some(node) = env.safekeepers.iter().find(|node| node.id == id) {
        Ok(SafekeeperNode::from_env(env, node))
    } else {
        bail!("could not find safekeeper '{}'", id)
    }
}

fn handle_safekeeper(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let (sub_name, sub_args) = match sub_match.subcommand() {
        Some(safekeeper_command_data) => safekeeper_command_data,
        None => bail!("no safekeeper subcommand provided"),
    };

    // All the commands take an optional safekeeper name argument
    let sk_id = if let Some(id_str) = sub_args.value_of("id") {
        ZNodeId(id_str.parse().context("while parsing safekeeper id")?)
    } else {
        DEFAULT_SAFEKEEPER_ID
    };
    let safekeeper = get_safekeeper(env, sk_id)?;

    match sub_name {
        "start" => {
            if let Err(e) = safekeeper.start() {
                eprintln!("safekeeper start failed: {}", e);
                exit(1);
            }
        }

        "stop" => {
            let immediate = sub_args.value_of("stop-mode") == Some("immediate");

            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {}", e);
                exit(1);
            }
        }

        "restart" => {
            let immediate = sub_args.value_of("stop-mode") == Some("immediate");

            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {}", e);
                exit(1);
            }

            if let Err(e) = safekeeper.start() {
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

fn handle_start_all(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let pageserver = PageServerNode::from_env(env);

    // Postgres nodes are not started automatically

    if let Err(e) = pageserver.start(&pageserver_config_overrides(sub_match)) {
        eprintln!("pageserver start failed: {}", e);
        exit(1);
    }

    for node in env.safekeepers.iter() {
        let safekeeper = SafekeeperNode::from_env(env, node);
        if let Err(e) = safekeeper.start() {
            eprintln!("safekeeper '{}' start failed: {}", safekeeper.id, e);
            exit(1);
        }
    }
    Ok(())
}

fn handle_stop_all(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let immediate = sub_match.value_of("stop-mode") == Some("immediate");

    let pageserver = PageServerNode::from_env(env);

    // Stop all compute nodes
    let cplane = ComputeControlPlane::load(env.clone())?;
    for (_k, node) in cplane.nodes {
        if let Err(e) = node.stop(false) {
            eprintln!("postgres stop failed: {}", e);
        }
    }

    if let Err(e) = pageserver.stop(immediate) {
        eprintln!("pageserver stop failed: {}", e);
    }

    for node in env.safekeepers.iter() {
        let safekeeper = SafekeeperNode::from_env(env, node);
        if let Err(e) = safekeeper.stop(immediate) {
            eprintln!("safekeeper '{}' stop failed: {}", safekeeper.id, e);
        }
    }
    Ok(())
}
