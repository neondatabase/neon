use clap::{App, SubCommand};
use std::process::exit;

use control_plane::{local_env, storage};

fn main() {
    let matches = App::new("zenith")
        .subcommand(SubCommand::with_name("init"))
        .subcommand(SubCommand::with_name("start"))
        .subcommand(SubCommand::with_name("stop"))
        .subcommand(SubCommand::with_name("status"))
        .subcommand(
            SubCommand::with_name("pg")
                .about("Manage postgres instances")
                .subcommand(SubCommand::with_name("create"))
                .subcommand(SubCommand::with_name("start"))
                .subcommand(SubCommand::with_name("stop"))
                .subcommand(SubCommand::with_name("destroy")),
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
            panic!() /* init was handled before */
        }

        ("start", Some(_sub_m)) => {
            let pageserver = storage::PageServerNode::from_env(&env);

            if let Err(e) = pageserver.start() {
                eprintln!("start: {}", e);
                exit(1);
            }

            // TODO: check and await actual start
            println!("Done!");
        }

        ("stop", Some(_sub_m)) => {
            let pageserver = storage::PageServerNode::from_env(&env);

            if let Err(e) = pageserver.stop() {
                eprintln!("stop: {}", e);
                exit(1);
            }

            println!("Done!");
        }

        ("status", Some(_sub_m)) => {}

        ("pg", Some(pg_match)) => {
            match pg_match.subcommand() {
                ("start", Some(_sub_m)) => {
                    println!("xxx: pg start");
                    // Ok(())
                }
                _ => {}
            }
        }
        _ => {}
    }
}
