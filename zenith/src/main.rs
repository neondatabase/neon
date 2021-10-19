use anyhow::anyhow;
use anyhow::{Context, Result};
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use control_plane::compute::ComputeControlPlane;
use control_plane::local_env;
use control_plane::storage::PageServerNode;
use pageserver::defaults::{DEFAULT_HTTP_LISTEN_PORT, DEFAULT_PG_LISTEN_PORT};
use std::collections::HashMap;
use std::process::exit;
use std::str::FromStr;
use zenith_utils::auth::{encode_from_key_path, Claims, Scope};
use zenith_utils::postgres_backend::AuthType;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use pageserver::branches::BranchInfo;

///
/// Branches tree element used as a value in the HashMap.
///
struct BranchTreeEl {
    /// `BranchInfo` received from the `pageserver` via the `branch_list` libpq API call.
    pub info: BranchInfo,
    /// Holds all direct children of this branch referenced using `timeline_id`.
    pub children: Vec<String>,
}

// Main entry point for the 'zenith' CLI utility
//
// This utility helps to manage zenith installation. That includes following:
//   * Management of local postgres installations running on top of the
//     pageserver.
//   * Providing CLI api to the pageserver
//   * TODO: export/import to/from usual postgres
fn main() -> Result<()> {
    let node_arg = Arg::with_name("node")
        .index(1)
        .help("Node name")
        .required(true);

    let timeline_arg = Arg::with_name("timeline")
        .index(2)
        .help("Branch name or a point-in time specification")
        .required(false);

    let tenantid_arg = Arg::with_name("tenantid")
        .long("tenantid")
        .help("Tenant id. Represented as a hexadecimal string 32 symbols length")
        .takes_value(true)
        .required(false);

    let port_arg = Arg::with_name("port")
        .long("port")
        .required(false)
        .value_name("port");

    let matches = App::new("Zenith CLI")
        .setting(AppSettings::ArgRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("init")
                .about("Initialize a new Zenith repository")
                .arg(
                    Arg::with_name("pageserver-pg-port")
                        .long("pageserver-pg-port")
                        .required(false)
                        .value_name("pageserver-pg-port"),
                )
                .arg(
                    Arg::with_name("pageserver-http-port")
                        .long("pageserver-http-port")
                        .required(false)
                        .value_name("pageserver-http-port"),
                )
                .arg(
                    Arg::with_name("enable-auth")
                        .long("enable-auth")
                        .takes_value(false)
                        .help("Enable authentication using ZenithJWT")
                ),
        )
        .subcommand(
            SubCommand::with_name("branch")
                .about("Create a new branch")
                .arg(Arg::with_name("branchname").required(false).index(1))
                .arg(Arg::with_name("start-point").required(false).index(2))
                .arg(tenantid_arg.clone()),
        ).subcommand(
            SubCommand::with_name("tenant")
            .setting(AppSettings::ArgRequiredElseHelp)
            .about("Manage tenants")
            .subcommand(SubCommand::with_name("list"))
            .subcommand(SubCommand::with_name("create").arg(Arg::with_name("tenantid").required(false).index(1)))
        )
        .subcommand(SubCommand::with_name("status"))
        .subcommand(SubCommand::with_name("start").about("Start local pageserver"))
        .subcommand(SubCommand::with_name("stop").about("Stop local pageserver")
                    .arg(Arg::with_name("immediate")
                    .help("Don't flush repository data at shutdown")
                    .required(false)
                    )
        )
        .subcommand(SubCommand::with_name("restart").about("Restart local pageserver"))
        .subcommand(
            SubCommand::with_name("pg")
                .setting(AppSettings::ArgRequiredElseHelp)
                .about("Manage postgres instances")
                .subcommand(SubCommand::with_name("list").arg(tenantid_arg.clone()))
                .subcommand(SubCommand::with_name("create")
                    .about("Create a postgres compute node")
                    .arg(node_arg.clone())
                    .arg(timeline_arg.clone())
                    .arg(tenantid_arg.clone())
                    .arg(port_arg.clone())
                    .arg(
                        Arg::with_name("config-only")
                            .help("Don't do basebackup, create compute node with only config files")
                            .long("config-only")
                            .required(false)
                    ))
                .subcommand(SubCommand::with_name("start")
                    .about("Start a postgres compute node.\n This command actually creates new node from scratch, but preserves existing config files")
                    .arg(node_arg.clone())
                    .arg(timeline_arg.clone())
                    .arg(tenantid_arg.clone())
                    .arg(port_arg.clone()))
                .subcommand(
                    SubCommand::with_name("stop")
                        .arg(node_arg.clone())
                        .arg(timeline_arg.clone())
                        .arg(tenantid_arg.clone())
                        .arg(
                            Arg::with_name("destroy")
                                .help("Also delete data directory (now optional, should be default in future)")
                                .long("destroy")
                                .required(false)
                        )
                    )

        )
        .get_matches();

    // Create config file
    if let ("init", Some(init_match)) = matches.subcommand() {
        let tenantid = ZTenantId::generate();
        let pageserver_pg_port = match init_match.value_of("pageserver-pg-port") {
            Some(v) => v.parse()?,
            None => DEFAULT_PG_LISTEN_PORT,
        };
        let pageserver_http_port = match init_match.value_of("pageserver-http-port") {
            Some(v) => v.parse()?,
            None => DEFAULT_HTTP_LISTEN_PORT,
        };

        let auth_type = if init_match.is_present("enable-auth") {
            AuthType::ZenithJWT
        } else {
            AuthType::Trust
        };

        local_env::init(
            pageserver_pg_port,
            pageserver_http_port,
            tenantid,
            auth_type,
        )
        .with_context(|| "Failed to create config file")?;
    }

    // all other commands would need config
    let env = match local_env::load_config() {
        Ok(conf) => conf,
        Err(e) => {
            eprintln!("Error loading config: {}", e);
            exit(1);
        }
    };

    match matches.subcommand() {
        ("init", Some(init_match)) => {
            let pageserver = PageServerNode::from_env(&env);
            if let Err(e) = pageserver.init(
                Some(&env.tenantid.to_string()),
                init_match.is_present("enable-auth"),
            ) {
                eprintln!("pageserver init failed: {}", e);
                exit(1);
            }
        }
        ("tenant", Some(args)) => {
            if let Err(e) = handle_tenant(args, &env) {
                eprintln!("tenant command failed: {}", e);
                exit(1);
            }
        }

        ("branch", Some(sub_args)) => {
            if let Err(e) = handle_branch(sub_args, &env) {
                eprintln!("branch command failed: {}", e);
                exit(1);
            }
        }

        ("start", Some(_sub_m)) => {
            let pageserver = PageServerNode::from_env(&env);

            if let Err(e) = pageserver.start() {
                eprintln!("pageserver start failed: {}", e);
                exit(1);
            }
        }

        ("stop", Some(stop_match)) => {
            let pageserver = PageServerNode::from_env(&env);

            let immediate = stop_match.is_present("immediate");

            if let Err(e) = pageserver.stop(immediate) {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }
        }

        ("restart", Some(_sub_m)) => {
            let pageserver = PageServerNode::from_env(&env);

            //TODO what shutdown strategy should we use here?
            if let Err(e) = pageserver.stop(false) {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }

            if let Err(e) = pageserver.start() {
                eprintln!("pageserver start failed: {}", e);
                exit(1);
            }
        }

        ("status", Some(_sub_m)) => {}

        ("pg", Some(pg_match)) => {
            if let Err(e) = handle_pg(pg_match, &env) {
                eprintln!("pg operation failed: {:?}", e);
                exit(1);
            }
        }

        _ => {}
    };

    Ok(())
}

///
/// Prints branches list as a tree-like structure.
///
fn print_branches_tree(branches: Vec<BranchInfo>) -> Result<()> {
    let mut branches_hash: HashMap<String, BranchTreeEl> = HashMap::new();

    // Form a hash table of branch timeline_id -> BranchTreeEl.
    for branch in &branches {
        branches_hash.insert(
            branch.timeline_id.to_string(),
            BranchTreeEl {
                info: branch.clone(),
                children: Vec::new(),
            },
        );
    }

    // Memorize all direct children of each branch.
    for branch in &branches {
        if let Some(tid) = &branch.ancestor_id {
            branches_hash
                .get_mut(tid)
                .with_context(|| "missing branch info in the HashMap")?
                .children
                .push(branch.timeline_id.to_string());
        }
    }

    // Sort children by tid to bring some minimal order.
    for branch in &mut branches_hash.values_mut() {
        branch.children.sort();
    }

    for branch in branches_hash.values() {
        // Start with root branches (no ancestors) first.
        // Now there is 'main' branch only, but things may change.
        if branch.info.ancestor_id.is_none() {
            print_branch(0, &Vec::from([true]), branch, &branches_hash)?;
        }
    }

    Ok(())
}

///
/// Recursively prints branch info with all its children.
///
fn print_branch(
    nesting_level: usize,
    is_last: &[bool],
    branch: &BranchTreeEl,
    branches: &HashMap<String, BranchTreeEl>,
) -> Result<()> {
    // Draw main padding
    print!(" ");

    if nesting_level > 0 {
        let lsn = branch
            .info
            .ancestor_lsn
            .as_ref()
            .with_context(|| "missing branch info in the HashMap")?;
        let mut br_sym = "┣━";

        // Draw each nesting padding with proper style
        // depending on whether its branch ended or not.
        if nesting_level > 1 {
            for l in &is_last[1..is_last.len() - 1] {
                if *l {
                    print!("   ");
                } else {
                    print!("┃  ");
                }
            }
        }

        // We are the last in this sub-branch
        if *is_last.last().unwrap() {
            br_sym = "┗━";
        }

        print!("{} @{}: ", br_sym, lsn);
    }

    // Finally print a branch name with new line
    println!("{}", branch.info.name);

    let len = branch.children.len();
    let mut i: usize = 0;
    let mut is_last_new = Vec::from(is_last);
    is_last_new.push(false);

    for child in &branch.children {
        i += 1;

        // Mark that the last padding is the end of the branch
        if i == len {
            if let Some(last) = is_last_new.last_mut() {
                *last = true;
            }
        }

        print_branch(
            nesting_level + 1,
            &is_last_new,
            branches
                .get(child)
                .with_context(|| "missing branch info in the HashMap")?,
            branches,
        )?;
    }

    Ok(())
}

/// Returns a map of timeline IDs to branch_name@lsn strings.
/// Connects to the pageserver to query this information.
fn get_branch_infos(
    env: &local_env::LocalEnv,
    tenantid: &ZTenantId,
) -> Result<HashMap<ZTimelineId, BranchInfo>> {
    let page_server = PageServerNode::from_env(env);
    let branch_infos: Vec<BranchInfo> = page_server.branch_list(tenantid)?;
    let branch_infos: HashMap<ZTimelineId, BranchInfo> = branch_infos
        .into_iter()
        .map(|branch_info| (branch_info.timeline_id, branch_info))
        .collect();

    Ok(branch_infos)
}

fn handle_tenant(tenant_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let pageserver = PageServerNode::from_env(env);
    match tenant_match.subcommand() {
        ("list", Some(_)) => {
            for tenant in pageserver.tenant_list()? {
                println!("{}", tenant);
            }
        }
        ("create", Some(create_match)) => {
            let tenantid = match create_match.value_of("tenantid") {
                Some(tenantid) => ZTenantId::from_str(tenantid)?,
                None => ZTenantId::generate(),
            };
            println!("using tenant id {}", tenantid);
            pageserver.tenant_create(tenantid)?;
            println!("tenant successfully created on the pageserver");
        }
        _ => {}
    }
    Ok(())
}

fn handle_branch(branch_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let pageserver = PageServerNode::from_env(env);

    if let Some(branchname) = branch_match.value_of("branchname") {
        let startpoint_str = branch_match
            .value_of("start-point")
            .ok_or_else(|| anyhow!("Missing start-point"))?;
        let tenantid: ZTenantId = branch_match
            .value_of("tenantid")
            .map_or(Ok(env.tenantid), |value| value.parse())?;
        let branch = pageserver.branch_create(branchname, startpoint_str, &tenantid)?;
        println!(
            "Created branch '{}' at {:?} for tenant: {}",
            branch.name, branch.latest_valid_lsn, tenantid,
        );
    } else {
        let tenantid: ZTenantId = branch_match
            .value_of("tenantid")
            .map_or(Ok(env.tenantid), |value| value.parse())?;
        // No arguments, list branches for tenant
        let branches = pageserver.branch_list(&tenantid)?;
        print_branches_tree(branches)?;
    }

    Ok(())
}

fn handle_pg(pg_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let mut cplane = ComputeControlPlane::load(env.clone())?;

    match pg_match.subcommand() {
        ("list", Some(list_match)) => {
            let tenantid: ZTenantId = list_match
                .value_of("tenantid")
                .map_or(Ok(env.tenantid), |value| value.parse())?;

            let branch_infos = get_branch_infos(env, &tenantid).unwrap_or_else(|e| {
                eprintln!("Failed to load branch info: {}", e);
                HashMap::new()
            });

            println!("NODE\tADDRESS\t\tBRANCH\tLSN\t\tSTATUS");
            for ((_, node_name), node) in cplane
                .nodes
                .iter()
                .filter(|((node_tenantid, _), _)| node_tenantid == &tenantid)
            {
                // FIXME: This shows the LSN at the end of the timeline. It's not the
                // right thing to do for read-only nodes that might be anchored at an
                // older point in time, or following but lagging behind the primary.
                let lsn_str = branch_infos
                    .get(&node.timelineid)
                    .map(|bi| bi.latest_valid_lsn.to_string())
                    .unwrap_or_else(|| "?".to_string());

                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    node_name,
                    node.address,
                    node.timelineid, // FIXME: resolve human-friendly branch name
                    lsn_str,
                    node.status(),
                );
            }
        }
        ("create", Some(create_match)) => {
            let tenantid: ZTenantId = create_match
                .value_of("tenantid")
                .map_or(Ok(env.tenantid), |value| value.parse())?;
            let node_name = create_match.value_of("node").unwrap_or("main");
            let timeline_name = create_match.value_of("timeline").unwrap_or(node_name);

            let port: Option<u16> = match create_match.value_of("port") {
                Some(p) => Some(p.parse()?),
                None => None,
            };
            cplane.new_node(tenantid, node_name, timeline_name, port)?;
        }
        ("start", Some(start_match)) => {
            let tenantid: ZTenantId = start_match
                .value_of("tenantid")
                .map_or(Ok(env.tenantid), |value| value.parse())?;
            let node_name = start_match.value_of("node").unwrap_or("main");
            let timeline_name = start_match.value_of("timeline");

            let port: Option<u16> = match start_match.value_of("port") {
                Some(p) => Some(p.parse()?),
                None => None,
            };

            let node = cplane.nodes.get(&(tenantid, node_name.to_owned()));

            let auth_token = if matches!(env.auth_type, AuthType::ZenithJWT) {
                let claims = Claims::new(Some(tenantid), Scope::Tenant);
                Some(encode_from_key_path(&claims, &env.private_key_path)?)
            } else {
                None
            };

            if let Some(node) = node {
                if timeline_name.is_some() {
                    println!("timeline name ignored because node exists already");
                }
                println!("Starting existing postgres {}...", node_name);
                node.start(&auth_token)?;
            } else {
                // when used with custom port this results in non obvious behaviour
                // port is remembered from first start command, i e
                // start --port X
                // stop
                // start <-- will also use port X even without explicit port argument
                let timeline_name = timeline_name.unwrap_or(node_name);
                println!(
                    "Starting new postgres {} on {}...",
                    node_name, timeline_name
                );
                let node = cplane.new_node(tenantid, node_name, timeline_name, port)?;
                node.start(&auth_token)?;
            }
        }
        ("stop", Some(stop_match)) => {
            let node_name = stop_match.value_of("node").unwrap_or("main");
            let destroy = stop_match.is_present("destroy");
            let tenantid: ZTenantId = stop_match
                .value_of("tenantid")
                .map_or(Ok(env.tenantid), |value| value.parse())?;

            let node = cplane
                .nodes
                .get(&(tenantid, node_name.to_owned()))
                .ok_or_else(|| anyhow!("postgres {} is not found", node_name))?;
            node.stop(destroy)?;
        }

        _ => {}
    }

    Ok(())
}
