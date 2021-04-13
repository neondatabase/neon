use clap::{App, ArgMatches, SubCommand, Arg};
use std::process::exit;
use std::error;

use control_plane::{compute::ComputeControlPlane, local_env, storage};

type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

fn main() {
    let name_arg = Arg::with_name("NAME")
        .short("n")
        .index(1)
        .help("name of this postgres instance")
        .required(true);
    let matches = App::new("zenith")
        .subcommand(SubCommand::with_name("init"))
        .subcommand(SubCommand::with_name("start"))
        .subcommand(SubCommand::with_name("stop"))
        .subcommand(SubCommand::with_name("status"))
        .subcommand(
            SubCommand::with_name("pg")
                .about("Manage postgres instances")
                .subcommand(SubCommand::with_name("create")
                    // .arg(name_arg.clone()
                    //     .required(false)
                    //     .help("name of this postgres instance (will be pgN if omitted)"))
                    )
                .subcommand(SubCommand::with_name("list"))
                .subcommand(SubCommand::with_name("start")
                    .arg(name_arg.clone()))
                .subcommand(SubCommand::with_name("stop")
                    .arg(name_arg.clone()))
                .subcommand(SubCommand::with_name("destroy")
                    .arg(name_arg.clone()))
        )
        .subcommand(
            SubCommand::with_name("snapshot")
                .about("Manage database snapshots")
                .subcommand(SubCommand::with_name("create"))
                .subcommand(SubCommand::with_name("start"))
                .subcommand(SubCommand::with_name("stop"))
                .subcommand(SubCommand::with_name("destroy")),
        )
        .get_matches();

    // handle init separately and exit
    if let Some("init") = matches.subcommand_name() {
        match local_env::init() {
            Ok(_) => {
                println!("Initialization complete! You may start zenith with 'zenith start' now.");
                exit(0);
            }
            Err(e) => {
                eprintln!("Error during init: {}", e);
                exit(1);
            }
        }
    }

    // all other commands would need config
    let env = match local_env::load_config() {
        Ok(conf) => conf,
        Err(e) => {
            eprintln!("Error loading config from ~/.zenith: {}", e);
            exit(1);
        }
    };

    match matches.subcommand() {
        ("init", Some(_)) => {
            panic!() /* Should not happen. Init was handled before */
        }

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
            if let Err(e) = handle_pg(pg_match, &env){
                eprintln!("pg operation failed: {}", e);
                exit(1);
            }
        }
        _ => {}
    }
}

fn handle_pg(pg_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let mut cplane = ComputeControlPlane::load(env.clone())?;

    match pg_match.subcommand() {
        ("create", Some(_sub_m)) => {
            cplane.new_node()?;
        }
        ("list", Some(_sub_m)) => {
            println!("NODE\tADDRESS\tSTATUS");
            for (node_name, node) in cplane.nodes.iter() {
                println!("{}\t{}\t{}", node_name, node.address, node.status());
            }
        }
        ("start", Some(sub_m)) => {
            let name = sub_m.value_of("NAME").unwrap();
            let node = cplane
                .nodes
                .get(name)
                .ok_or(format!("postgres {} is not found", name))?;
            node.start()?;
        }
        ("stop", Some(sub_m)) => {
            let name = sub_m.value_of("NAME").unwrap();
            let node = cplane
                .nodes
                .get(name)
                .ok_or(format!("postgres {} is not found", name))?;
            node.stop()?;
        }

        _ => {}
    }

    Ok(())
}
