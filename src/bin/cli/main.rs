use clap::{App, AppSettings};
use anyhow::{Result};

mod subcommand;
pub mod pg;
pub mod storage;
pub mod snapshot;

fn main() -> Result<()> {

    let cli_commands = subcommand::ClapCommands {
        commands: vec![
            Box::new(pg::PgCmd {
                clap_cmd: clap::SubCommand::with_name("pg"),
            }),
            Box::new(storage::StorageCmd {
                clap_cmd: clap::SubCommand::with_name("storage"),
            }),
            Box::new(snapshot::SnapshotCmd {
                clap_cmd: clap::SubCommand::with_name("snapshot"),
            }),
        ],
    };


    let matches = App::new("zenith")
        .about("Zenith CLI")
        .version("1.0")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommands(cli_commands.generate())
        .get_matches();


    if let Some(subcommand) = matches.subcommand_name() {
        println!("'git {}' was used", subcommand);
    }

    match matches.subcommand() {
        ("pg", Some(sub_args)) => cli_commands.commands[0].run(sub_args.clone())?,
        ("storage", Some(sub_args)) => cli_commands.commands[1].run(sub_args.clone())?,
        ("snapshot", Some(sub_args)) => cli_commands.commands[2].run(sub_args.clone())?,
        ("", None) => println!("No subcommand"),
        _ => unreachable!(),
    }
    Ok(())
}
