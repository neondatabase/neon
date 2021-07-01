use anyhow::{anyhow, bail};
use anyhow::{Context, Result};
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use control_plane::compute::ComputeControlPlane;
use control_plane::local_env::{self, LocalEnv};
use control_plane::storage::PageServerNode;
use std::collections::btree_map::Entry;
use std::collections::HashMap;
use std::process::exit;
use std::str::FromStr;

use pageserver::{branches::BranchInfo, ZTimelineId};
use zenith_utils::lsn::Lsn;

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
//   * Providing CLI api to the pageserver (local or remote)
//   * TODO: export/import to/from usual postgres
fn main() -> Result<()> {
    let timeline_arg = Arg::with_name("timeline")
        .short("n")
        .index(1)
        .help("timeline name")
        .required(true);

    let matches = App::new("Zenith CLI")
        .setting(AppSettings::ArgRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("init")
                .about("Initialize a new Zenith repository")
                .arg(
                    Arg::with_name("remote-pageserver")
                        .long("remote-pageserver")
                        .required(false)
                        .value_name("pageserver-url"),
                )
                .arg(
                    Arg::with_name("snapshot-path")
                        .long("snapshot-path")
                        .required(false)
                        .value_name("snapshot-path")
                        .help("Source to init repository from")
                        ,
                ),
        )
        .subcommand(
            SubCommand::with_name("branch")
                .about("Create a new branch")
                .arg(Arg::with_name("branchname").required(false).index(1))
                .arg(Arg::with_name("start-point").required(false).index(2)),
        )
        .subcommand(SubCommand::with_name("status"))
        .subcommand(SubCommand::with_name("start").about("Start local pageserver"))
        .subcommand(SubCommand::with_name("stop").about("Stop local pageserver"))
        .subcommand(SubCommand::with_name("restart").about("Restart local pageserver"))
        .subcommand(
            SubCommand::with_name("pg")
                .setting(AppSettings::ArgRequiredElseHelp)
                .about("Manage postgres instances")
                .subcommand(SubCommand::with_name("list"))
                .subcommand(SubCommand::with_name("create").arg(timeline_arg.clone()))
                .subcommand(SubCommand::with_name("start").arg(timeline_arg.clone()))
                .subcommand(
                    SubCommand::with_name("stop")
                        .arg(timeline_arg.clone())
                        .arg(
                            Arg::with_name("destroy")
                                .help("Also delete data directory (now optional, should be default in future)")
                                .long("destroy")
                                .required(false)
                        )
                )
        )
        .subcommand(
            SubCommand::with_name("remote")
                .setting(AppSettings::ArgRequiredElseHelp)
                .about("Manage remote pagerservers")
                .subcommand(
                    SubCommand::with_name("add")
                        .about("Add a new remote pageserver")
                        .arg(Arg::with_name("name").required(true))
                        .arg(
                            Arg::with_name("url")
                                .help("PostgreSQL connection URI")
                                .required(true),
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("push")
                .about("Push timeline to remote pageserver")
                .arg(Arg::with_name("timeline").required(true))
                .arg(Arg::with_name("remote").required(true)),
        )
        .get_matches();

    // Create config file
    if let ("init", Some(sub_args)) = matches.subcommand() {
        let pageserver_uri = sub_args.value_of("pageserver-url");
        local_env::init(pageserver_uri).with_context(|| "Failed to create cofig file")?;
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
        ("init", Some(sub_args)) => {
            let snapshot_path = sub_args.value_of("snapshot-path");

            let pageserver = PageServerNode::from_env(&env);
            if let Err(e) = pageserver.init(snapshot_path) {
                eprintln!("pageserver init failed: {}", e);
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

        ("stop", Some(_sub_m)) => {
            let pageserver = PageServerNode::from_env(&env);

            if let Err(e) = pageserver.stop() {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }
        }

        ("restart", Some(_sub_m)) => {
            let pageserver = PageServerNode::from_env(&env);

            if let Err(e) = pageserver.stop() {
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
                eprintln!("pg operation failed: {}", e);
                exit(1);
            }
        }

        ("remote", Some(remote_match)) => {
            if let Err(e) = handle_remote(remote_match, &env) {
                eprintln!("remote operation failed: {}", e);
                exit(1);
            }
        }

        ("push", Some(push_match)) => {
            if let Err(e) = handle_push(push_match, &env) {
                eprintln!("push operation failed: {}", e);
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
fn get_branch_infos(env: &local_env::LocalEnv) -> Result<HashMap<ZTimelineId, BranchInfo>> {
    let page_server = PageServerNode::from_env(env);
    let branch_infos: Vec<BranchInfo> = page_server.branches_list()?;
    let branch_infos: HashMap<ZTimelineId, BranchInfo> = branch_infos
        .into_iter()
        .map(|branch_info| (branch_info.timeline_id, branch_info))
        .collect();

    Ok(branch_infos)
}

fn handle_branch(branch_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let pageserver = PageServerNode::from_env(&env);

    if let Some(branchname) = branch_match.value_of("branchname") {
        if let Some(startpoint_str) = branch_match.value_of("start-point") {
            let branch = pageserver.branch_create(branchname, startpoint_str)?;
            println!(
                "Created branch '{}' at {:?}",
                branch.name,
                branch.latest_valid_lsn.unwrap_or(Lsn(0))
            );
        } else {
            bail!("Missing start-point");
        }
    } else {
        // No arguments, list branches
        let branches = pageserver.branches_list()?;
        print_branches_tree(branches)?;
    }

    Ok(())
}

fn handle_pg(pg_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let mut cplane = ComputeControlPlane::load(env.clone())?;

    match pg_match.subcommand() {
        ("list", Some(_sub_m)) => {
            let branch_infos = get_branch_infos(env).unwrap_or_else(|e| {
                eprintln!("Failed to load branch info: {}", e);
                HashMap::new()
            });

            println!("BRANCH\tADDRESS\t\tLSN\t\tSTATUS");
            for (timeline_name, node) in cplane.nodes.iter() {
                println!(
                    "{}\t{}\t{}\t{}",
                    timeline_name,
                    node.address,
                    branch_infos
                        .get(&node.timelineid)
                        .map(|bi| bi
                            .latest_valid_lsn
                            .map_or("?".to_string(), |lsn| lsn.to_string()))
                        .unwrap_or_else(|| "?".to_string()),
                    node.status(),
                );
            }
        }
        ("create", Some(sub_m)) => {
            let timeline_name = sub_m.value_of("timeline").unwrap_or("main");
            cplane.new_node(timeline_name)?;
        }
        ("start", Some(sub_m)) => {
            let timeline_name = sub_m.value_of("timeline").unwrap_or("main");

            let node = cplane.nodes.get(timeline_name);

            println!("Starting postgres on timeline {}...", timeline_name);
            if let Some(node) = node {
                node.start()?;
            } else {
                let node = cplane.new_node(timeline_name)?;
                node.start()?;
            }
        }
        ("stop", Some(sub_m)) => {
            let timeline_name = sub_m.value_of("timeline").unwrap_or("main");
            let destroy = sub_m.is_present("destroy");

            let node = cplane
                .nodes
                .get(timeline_name)
                .ok_or_else(|| anyhow!("postgres {} is not found", timeline_name))?;
            node.stop(destroy)?;
        }

        _ => {}
    }

    Ok(())
}

fn handle_remote(remote_match: &ArgMatches, local_env: &LocalEnv) -> Result<()> {
    match remote_match.subcommand() {
        ("add", Some(args)) => {
            let name = args.value_of("name").unwrap();
            let url = args.value_of("url").unwrap();

            // validate the URL
            postgres::Config::from_str(url)?;

            let mut new_local_env = local_env.clone();

            match new_local_env.remotes.entry(name.to_string()) {
                Entry::Vacant(vacant) => {
                    vacant.insert(url.to_string());
                }
                Entry::Occupied(_) => bail!("origin '{}' already exists", name),
            }

            local_env::save_config(&new_local_env)?;
        }
        _ => bail!("unknown command"),
    }

    Ok(())
}

fn handle_push(push_match: &ArgMatches, local_env: &LocalEnv) -> Result<()> {
    let timeline_id_str = push_match.value_of("timeline").unwrap();
    ZTimelineId::from_str(timeline_id_str)?;

    let remote_name = push_match.value_of("remote").unwrap();
    let remote = local_env
        .remotes
        .get(remote_name)
        .ok_or_else(|| anyhow!("remote {} not found", remote_name))?;

    let page_server = PageServerNode::from_env(local_env);
    let mut client = page_server.page_server_psql_client()?;
    client.simple_query(&format!("request_push {} {}", timeline_id_str, remote))?;

    Ok(())
}
