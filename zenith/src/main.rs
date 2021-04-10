use clap::{App, SubCommand};
use std::fs;
use std::process::exit;
use std::process::Command;

use control_plane::local_env;

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
    let conf = match local_env::load_config() {
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
            println!(
                "Starting pageserver at '{}'",
                conf.pageserver.listen_address
            );

            let status = Command::new(conf.zenith_distrib_dir.join("pageserver"))
                .args(&["-D", conf.data_dir.to_str().unwrap()])
                .args(&["-l", conf.pageserver.listen_address.to_string().as_str()])
                .arg("-d")
                .arg("--skip-recovery")
                .env_clear()
                .env("PATH", conf.pg_bin_dir().to_str().unwrap()) // pageserver needs postres-wal-redo binary
                .env("LD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
                .status()
                .expect("failed to start pageserver");

            if !status.success() {
                eprintln!(
                    "Pageserver failed to start. See '{}' for details.",
                    conf.pageserver_log().to_str().unwrap()
                );
                exit(1);
            }

            // TODO: check it's actually started, or run status

            println!("Done!");
        }

        ("stop", Some(_sub_m)) => {
            let pid = fs::read_to_string(conf.pageserver_pidfile()).unwrap();
            let status = Command::new("kill")
                .arg(pid)
                .env_clear()
                .status()
                .expect("failed to execute kill");

            if !status.success() {
                eprintln!("Failed to kill pageserver");
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
