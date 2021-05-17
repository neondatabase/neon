use std::path::{Path, PathBuf};
use std::process::exit;
use std::str::FromStr;
use std::{collections::HashMap, fs};

use anyhow::{Result, Context};
use anyhow::{anyhow, bail};
use clap::{App, Arg, ArgMatches, SubCommand};

use control_plane::local_env::LocalEnv;
use control_plane::storage::PageServerNode;
use control_plane::{compute::ComputeControlPlane, local_env, storage};

use pageserver::{branches::BranchInfo, ZTimelineId};

use zenith_utils::lsn::Lsn;

fn zenith_repo_dir() -> PathBuf {
    // Find repository path
    match std::env::var_os("ZENITH_REPO_DIR") {
        Some(val) => PathBuf::from(val.to_str().unwrap()),
        None => ".zenith".into(),
    }
}

// Main entry point for the 'zenith' CLI utility
//
// This utility can used to work with a local zenith repository.
// In order to run queries in it, you need to launch the page server,
// and a compute node against the page server
fn main() -> Result<()> {
    let name_arg = Arg::with_name("NAME")
        .short("n")
        .index(1)
        .help("name of this postgres instance")
        .required(true);
    let matches = App::new("zenith")
        .about("Zenith CLI")
        .subcommand(SubCommand::with_name("init").about("Initialize a new Zenith repository"))
        .subcommand(
            SubCommand::with_name("branch")
                .about("Create a new branch")
                .arg(Arg::with_name("branchname").required(false).index(1))
                .arg(Arg::with_name("start-point").required(false).index(2)),
        )
        .subcommand(
            SubCommand::with_name("pageserver")
                .about("Manage pageserver instance")
                .subcommand(SubCommand::with_name("status"))
                .subcommand(SubCommand::with_name("start"))
                .subcommand(SubCommand::with_name("stop")),
        )
        .subcommand(
            SubCommand::with_name("pg")
                .about("Manage postgres instances")
                .subcommand(
                    SubCommand::with_name("create")
                        // .arg(name_arg.clone()
                        //     .required(false)
                        //     .help("name of this postgres instance (will be pgN if omitted)"))
                        .arg(Arg::with_name("timeline").required(false).index(1)),
                )
                .subcommand(SubCommand::with_name("list"))
                .subcommand(SubCommand::with_name("start").arg(name_arg.clone()))
                .subcommand(SubCommand::with_name("stop").arg(name_arg.clone()))
                .subcommand(SubCommand::with_name("destroy").arg(name_arg.clone())),
        )
        .get_matches();

    // handle init separately and exit
    if let ("init", Some(sub_args)) = matches.subcommand() {
        run_init_cmd(sub_args.clone())?;
        exit(0);
    }

    // all other commands would need config

    let repopath = zenith_repo_dir();
    if !repopath.exists() {
        bail!(
            "Zenith repository does not exist in {}.\n\
               Set ZENITH_REPO_DIR or initialize a new repository with 'zenith init'",
            repopath.display()
        );
    }
    // TODO: check that it looks like a zenith repository
    let env = match local_env::load_config(&repopath) {
        Ok(conf) => conf,
        Err(e) => {
            eprintln!("Error loading config from {}: {}", repopath.display(), e);
            exit(1);
        }
    };

    match matches.subcommand() {
        ("init", Some(_)) => {
            panic!() /* Should not happen. Init was handled before */
        }

        ("branch", Some(sub_args)) => run_branch_cmd(&env, sub_args.clone())?,
        ("pageserver", Some(sub_args)) => run_pageserver_cmd(&env, sub_args.clone())?,

        ("start", Some(_sub_m)) => {
            let pageserver = storage::PageServerNode::from_env(&env);

            if let Err(e) = pageserver.start() {
                eprintln!("pageserver start: {}", e);
                exit(1);
            }
        }

        ("stop", Some(_sub_m)) => {
            let pageserver = storage::PageServerNode::from_env(&env);
            if let Err(e) = pageserver.stop() {
                eprintln!("pageserver stop: {}", e);
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
        _ => {}
    };

    Ok(())
}

fn run_pageserver_cmd(local_env: &LocalEnv, args: ArgMatches) -> Result<()> {
    match args.subcommand() {
        ("status", Some(_sub_m)) => {
            todo!();
        }
        ("start", Some(_sub_m)) => {
            let psnode = PageServerNode::from_env(local_env);
            psnode.start()?;
            println!("Page server started");
        }
        ("stop", Some(_sub_m)) => {
            let psnode = PageServerNode::from_env(local_env);
            psnode.stop()?;
            println!("Page server stopped");
        }
        _ => unreachable!(),
    };

    Ok(())
}

// Peek into the repository, to grab the timeline ID of given branch
pub fn get_branch_timeline(repopath: &Path, branchname: &str) -> ZTimelineId {
    let branchpath = repopath.join("refs/branches/".to_owned() + branchname);

    ZTimelineId::from_str(&(fs::read_to_string(&branchpath).unwrap())).unwrap()
}

fn handle_pg(pg_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let mut cplane = ComputeControlPlane::load(env.clone())?;

    // FIXME: cheat and resolve the timeline by peeking into the
    // repository. In reality, when you're launching a compute node
    // against a possibly-remote page server, we wouldn't know what
    // branches exist in the remote repository. Or would we require
    // that you "zenith fetch" them into a local repoitory first?
    match pg_match.subcommand() {
        ("create", Some(sub_m)) => {
            let timeline_arg = sub_m.value_of("timeline").unwrap_or("main");
            let timeline = get_branch_timeline(&env.repo_path, timeline_arg);

            println!("Initializing Postgres on timeline {}...", timeline);

            cplane.new_node(timeline)?;
        }
        ("list", Some(_sub_m)) => {
            let page_server = storage::PageServerNode::from_env(env);
            let mut client = page_server.page_server_psql_client()?;
            let branches_msgs = client.simple_query("pg_list")?;

            let branches_json = branches_msgs
                .first()
                .map(|msg| match msg {
                    postgres::SimpleQueryMessage::Row(row) => row.get(0),
                    _ => None,
                })
                .flatten()
                .ok_or_else(|| anyhow!("missing branches"))?;

            let branch_infos: Vec<BranchInfo> = serde_json::from_str(branches_json)?;
            let branch_infos: Result<HashMap<ZTimelineId, String>> = branch_infos
                .into_iter()
                .map(|branch_info| {
                    let timeline_id = ZTimelineId::from_str(&branch_info.timeline_id)?;
                    let lsn_string_opt = branch_info.latest_valid_lsn.map(|lsn| lsn.to_string());
                    let lsn_str = lsn_string_opt.as_deref().unwrap_or("?");
                    let branch_lsn_string = format!("{}@{}", branch_info.name, lsn_str);
                    Ok((timeline_id, branch_lsn_string))
                })
                .collect();
            let branch_infos = branch_infos?;

            println!("NODE\tADDRESS\t\tSTATUS\tBRANCH@LSN");
            for (node_name, node) in cplane.nodes.iter() {
                println!(
                    "{}\t{}\t{}\t{}",
                    node_name,
                    node.address,
                    node.status(),
                    branch_infos
                        .get(&node.timelineid)
                        .map(|s| s.as_str())
                        .unwrap_or("?")
                );
            }
        }
        ("start", Some(sub_m)) => {
            let name = sub_m.value_of("NAME").unwrap();
            let node = cplane
                .nodes
                .get(name)
                .ok_or_else(|| anyhow!("postgres {} is not found", name))?;
            node.start()?;
        }
        ("stop", Some(sub_m)) => {
            let name = sub_m.value_of("NAME").unwrap();
            let node = cplane
                .nodes
                .get(name)
                .ok_or_else(|| anyhow!("postgres {} is not found", name))?;
            node.stop()?;
        }

        _ => {}
    }

    Ok(())
}

// "zenith init" - Initialize a new Zenith repository in current dir
fn run_init_cmd(_args: ArgMatches) -> Result<()> {
    local_env::init()?;
    Ok(())
}

// handle "zenith branch" subcommand
fn run_branch_cmd(local_env: &LocalEnv, args: ArgMatches) -> Result<()> {
    let repopath = local_env.repo_path.to_str().unwrap();

    if let Some(branchname) = args.value_of("branchname") {
        if PathBuf::from(format!("{}/refs/branches/{}", repopath, branchname)).exists() {
            anyhow::bail!("branch {} already exists", branchname);
        }

        if let Some(startpoint_str) = args.value_of("start-point") {
            let mut startpoint = parse_point_in_time(startpoint_str)?;

            if startpoint.lsn == Lsn(0) {
                // Find end of WAL on the old timeline
                let end_of_wal = local_env::find_end_of_wal(local_env, startpoint.timelineid)?;

                println!("branching at end of WAL: {}", end_of_wal);

                startpoint.lsn = end_of_wal;
            }

            return local_env::create_branch(local_env, branchname, startpoint);
        } else {
            panic!("Missing start-point");
        }
    } else {
        // No arguments, list branches
        list_branches()?;
    }
    Ok(())
}

fn list_branches() -> Result<()> {
    // list branches
    let paths = fs::read_dir(zenith_repo_dir().join("refs").join("branches"))?;

    for path in paths {
        println!("  {}", path?.file_name().to_str().unwrap());
    }

    Ok(())
}

//
// Parse user-given string that represents a point-in-time.
//
// We support multiple variants:
//
// Raw timeline id in hex, meaning the end of that timeline:
//    bc62e7d612d0e6fe8f99a6dd2f281f9d
//
// A specific LSN on a timeline:
//    bc62e7d612d0e6fe8f99a6dd2f281f9d@2/15D3DD8
//
// Same, with a human-friendly branch name:
//    main
//    main@2/15D3DD8
//
// Human-friendly tag name:
//    mytag
//
//
fn parse_point_in_time(s: &str) -> Result<local_env::PointInTime> {
    let mut strings = s.split('@');
    let name = strings.next().unwrap();

    let lsn: Option<Lsn>;
    if let Some(lsnstr) = strings.next() {
        lsn = Some(
            Lsn::from_str(lsnstr)
                .with_context(|| "invalid LSN in point-in-time specification")?
                );
    } else {
        lsn = None
    }

    // Check if it's a tag
    if lsn.is_none() {
        let tagpath = zenith_repo_dir().join("refs").join("tags").join(name);
        if tagpath.exists() {
            let pointstr = fs::read_to_string(tagpath)?;

            return parse_point_in_time(&pointstr);
        }
    }
    // Check if it's a branch
    // Check if it's branch @ LSN
    let branchpath = zenith_repo_dir().join("refs").join("branches").join(name);
    if branchpath.exists() {
        let pointstr = fs::read_to_string(branchpath)?;

        let mut result = parse_point_in_time(&pointstr)?;

        result.lsn = lsn.unwrap_or(Lsn(0));
        return Ok(result);
    }

    // Check if it's a timelineid
    // Check if it's timelineid @ LSN
    let tlipath = zenith_repo_dir().join("timelines").join(name);
    if tlipath.exists() {
        let result = local_env::PointInTime {
            timelineid: ZTimelineId::from_str(name)?,
            lsn: lsn.unwrap_or(Lsn(0)),
        };

        return Ok(result);
    }

    panic!("could not parse point-in-time {}", s);
}
